#pragma once

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using socket_t = SOCKET;
using ssize_t =
    ptrdiff_t; // Standardizes POSIX return type for raw socket reads on Windows
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
#endif

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ezsock {

#ifdef _WIN32
constexpr socket_t invalid_fd = INVALID_SOCKET;
inline int get_error() { return WSAGetLastError(); }
inline void close_fd(socket_t s) { closesocket(s); }
#else
constexpr socket_t invalid_fd = -1;
inline int get_error() { return errno; }
inline void close_fd(socket_t s) { ::close(s); }
#endif

[[noreturn]] inline void throw_error(const char *what) {
      throw std::system_error(get_error(), std::system_category(), what);
}

#ifdef _WIN32
namespace detail {
struct WsaGuard {
      WsaGuard() {
            WSADATA w;
            WSAStartup(MAKEWORD(2, 2), &w);
      }
      ~WsaGuard() { WSACleanup(); }
};
inline WsaGuard _wsa_guard;
} // namespace detail
#endif

enum class Protocol : uint8_t { TCP, UDP };

// --- Fixed Wire Format Layout ---
#pragma pack(push, 1) // used to not apply invisible padding to the struct
struct PacketHeader {
      uint32_t client_id = 0;
      Protocol protocol = Protocol::TCP;
      uint8_t type = 0;
      uint32_t payload_size =
          0; // Informs receiver how many bytes to pull off the stream next
};
#pragma pack(pop) // restore default struct padding

// --- Safe Application Logic Layout ---
struct Packet {
      PacketHeader header;
      std::vector<std::byte> data; // Left outside pack(1) to avoid undefined
                                   // behavior with heap pointers
};

// --- Packet helpers ---
inline Packet make_packet(uint32_t client_id, Protocol proto, uint8_t type,
                          std::string_view s) {
      Packet pkt;
      pkt.header.client_id = client_id;
      pkt.header.protocol = proto;
      pkt.header.type = type;
      pkt.header.payload_size = static_cast<uint32_t>(s.size());
      pkt.data.assign(reinterpret_cast<const std::byte *>(s.data()),
                      reinterpret_cast<const std::byte *>(s.data() + s.size()));
      return pkt;
}

template <typename T>
      requires std::is_trivially_copyable_v<T>
inline Packet make_packet(uint32_t client_id, Protocol proto, uint8_t type,
                          const T &val) {
      Packet pkt;
      pkt.header.client_id = client_id;
      pkt.header.protocol = proto;
      pkt.header.type = type;
      auto bytes = std::as_bytes(std::span(&val, 1));
      pkt.header.payload_size = static_cast<uint32_t>(bytes.size());
      pkt.data.assign(bytes.begin(), bytes.end());
      return pkt;
}

class Socket {
    protected:
      socket_t fd = invalid_fd;

    public:
      ~Socket() {
            if (fd != invalid_fd)
                  close_fd(fd);
      }

      Socket(const Socket &) = delete;
      Socket &operator=(const Socket &) = delete;

      Socket(Socket &&other) noexcept : fd(other.fd) { other.fd = invalid_fd; }

      Socket &operator=(Socket &&other) noexcept {
            if (this != &other) {
                  if (fd != invalid_fd)
                        close_fd(fd);
                  fd = other.fd;
                  other.fd = invalid_fd;
            }
            return *this;
      }

    protected:
      Socket() = default;
      explicit Socket(socket_t fd) : fd(fd) {}
};

class Server : public Socket {
    public:
      using Validator = std::function<bool(const Packet &)>;

      explicit Server() : Socket() {}
      ~Server() = default;

      // config
      void start_tcp(uint16_t port);
      void start_udp(uint16_t port);
      void set_metadata_buffer_size(size_t size) {
            metadata_buffer_size = size;
      }

      // --- Handlers & Rules ---
      void register_handler(uint8_t type, std::function<void(Packet)> handler);

      template <typename E>
            requires std::is_enum_v<E>
      void register_handler(E type, std::function<void(Packet)> handler) {
            register_handler(static_cast<uint8_t>(type), handler);
      }

      void add_validator(Validator v) { validators.push_back(std::move(v)); }

      // --- Communication ---
      void send(Packet pkt);
      void broadcast(Packet pkt);
      void broadcast_except(std::span<const uint32_t> exclude, Packet pkt);

      // --- Lifecycle ---
      void on_connect(std::function<void(Packet)> cb);
      void on_disconnect(std::function<void(uint32_t)> cb);

      // --- Lobby & Admin ---
      void set_lobby_open(bool open) { lobby_open = open; }
      bool is_lobby_open() const { return lobby_open; }

      void kick(uint32_t id);
      std::string ban(uint32_t id);
      void unban_ip(std::string ip);

    private:
      void queue_packet(Packet pkt);

      void receiver_loop(std::stop_token stop);
      void sender_loop(std::stop_token stop);

      socket_t tcp_listener = invalid_fd;
      socket_t udp_socket = invalid_fd;

      std::unordered_map<uint32_t, socket_t> tcp_clients;
      std::unordered_map<uint32_t, sockaddr_storage> udp_clients;

      std::mutex clients_mutex;
      std::unordered_map<uint8_t, std::function<void(Packet)>> handlers;
      std::vector<Validator> validators;

      std::queue<Packet> outbound_queue;
      std::mutex queue_mutex;
      std::condition_variable_any
          queue_cv; // Changed to _any; works with
                    // std::jthread's stop_token unblocking

      std::function<void(Packet)> connect_callback;
      std::function<void(uint32_t)> disconnect_callback;

      uint32_t next_id = 0;
      bool lobby_open = true;
      size_t metadata_buffer_size = 1024;

      std::jthread receiver_thread;
      std::jthread sender_thread;

    public:
      std::unordered_set<std::string> banned_ips;
};

class Client {
    public:
      Client() = default;
      ~Client() = default;

      void join(std::string_view tcp_addr, std::string_view udp_addr,
                std::span<const std::byte> metadata);

      void register_handler(uint8_t type, std::function<void(Packet)> handler);

      template <typename E>
            requires std::is_enum_v<E>
      void register_handler(E type, std::function<void(Packet)> handler) {
            register_handler(static_cast<uint8_t>(type), handler);
      }

      void send(Packet pkt);

      void on_disconnect(std::function<void()> cb);
      void start(size_t buffer_size = 4096);

    private:
      socket_t tcp_sock = invalid_fd;
      socket_t udp_sock = invalid_fd;
      sockaddr_storage server_udp_addr{};
      socklen_t server_udp_addr_len = 0;
      uint32_t client_id = 0;

      std::unordered_map<uint8_t, std::function<void(Packet)>> handlers;
      std::function<void()> disconnect_callback;
      std::jthread receiver_thread;
};

// UPnP wrapper for miniupnpc library
struct PortMapping {
      uint16_t port;
      Protocol protocol;
      std::string description;
};

class UPnP {
    public:
      UPnP() = default;
      UPnP(std::initializer_list<PortMapping> mappings);
      ~UPnP();

      UPnP(const UPnP &) = delete;
      UPnP &operator=(const UPnP &) = delete;
      UPnP(UPnP &&) noexcept;
      UPnP &operator=(UPnP &&) noexcept;

    private:
      struct Impl;
      std::unique_ptr<Impl> impl_;
      std::vector<PortMapping> mappings_;
};

// utils
std::string get_local_ip();

} // namespace ezsock
