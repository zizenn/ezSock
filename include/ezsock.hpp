#pragma once

#include <cstdint>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using socket_t = SOCKET;
using ssize_t = ptrdiff_t;
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
#endif

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
inline WsaGuard _wsa_guard; // C++17 inline variable prevents duplicate symbols
} // namespace detail
#endif

// Safe, clean packet structures (No packing or alignment issues)
struct PacketHeader {
      uint32_t uniqueId;
      uint16_t type;
      uint16_t length;
};

struct Packet {
      PacketHeader header;
      std::vector<uint8_t> payload;

      // Helper to serialize this packet into a network-ready byte stream (Big
      // Endian)
      std::vector<uint8_t> serialize() const {
            std::vector<uint8_t> buffer;
            buffer.reserve(8 + payload.size());

            // Header serialization (Bit-shifting enforces Network Byte Order)
            buffer.push_back((header.uniqueId >> 24) & 0xFF);
            buffer.push_back((header.uniqueId >> 16) & 0xFF);
            buffer.push_back((header.uniqueId >> 8) & 0xFF);
            buffer.push_back(header.uniqueId & 0xFF);

            buffer.push_back((header.type >> 8) & 0xFF);
            buffer.push_back(header.type & 0xFF);

            buffer.push_back((header.length >> 8) & 0xFF);
            buffer.push_back(header.length & 0xFF);

            // Append payload payload data
            buffer.insert(buffer.end(), payload.begin(), payload.end());
            return buffer;
      }

      // Helper to parse a raw network stream back into a Packet
      static Packet deserialize(const std::vector<uint8_t> &buffer) {
            Packet packet{};
            if (buffer.size() < 8)
                  return packet;

            packet.header.uniqueId = (buffer[0] << 24) | (buffer[1] << 16) |
                                     (buffer[2] << 8) | buffer[3];
            packet.header.type = (buffer[4] << 8) | buffer[5];
            packet.header.length = (buffer[6] << 8) | buffer[7];

            if (buffer.size() >= 8 + packet.header.length) {
                  packet.payload.assign(buffer.begin() + 8,
                                        buffer.begin() + 8 +
                                              packet.header.length);
            }
            return packet;
      }
};

class Socket {
protected:
      socket_t fd = invalid_fd;

      Socket() = default;
      explicit Socket(socket_t fd) : fd(fd) {}

public:
      // Virtual destructor guarantees safe cleanup for derived classes
      virtual ~Socket() {
            if (fd != invalid_fd) {
                  close_fd(fd);
            }
      }

      // delete copy functions
      // (u cant copy jthreads, so we make sure u cant copy)
      Socket(const Socket &) = delete;
      Socket &operator=(const Socket &) = delete;

      // allows moving cuz its actually safe
      Socket(Socket &&other) noexcept
          : fd(std::exchange(other.fd, invalid_fd)) {}

      Socket &operator=(Socket &&other) noexcept {
            if (this != &other) {
                  if (fd != invalid_fd) {
                        close_fd(fd);
                  }
                  fd = std::exchange(other.fd, invalid_fd);
            }
            return *this;
      }

      // Accessors
      socket_t native_handle() const noexcept { return fd; }
      bool is_valid() const noexcept { return fd != invalid_fd; }
};

} // namespace ezsock
