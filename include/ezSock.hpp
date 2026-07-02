#pragma once

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using socket_t = SOCKET;
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
#endif

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>

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

// Throw a std::system_error with the current platform error
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

// socket class
class Socket {
protected:
  socket_t fd = invalid_fd; // default to invalid, owns nothing yet

public:
  // destructor — runs automatically when object goes out of scope
  ~Socket() {
    if (fd != invalid_fd)
      close_fd(fd);
  }

  // delete copy constructor — two objects must never own the same fd
  Socket(const Socket &) = delete;
  // delete copy assignment — same reason
  Socket &operator=(const Socket &) = delete;

  // move constructor — transfer ownership from other to this
  Socket(Socket &&other) noexcept : fd(other.fd) {
    other.fd = invalid_fd; // empty out the source
  }

  // move assignment — same idea but this already exists
  Socket &operator=(Socket &&other) noexcept {
    if (this != &other) { // guard against self-assignment
      if (fd != invalid_fd)
        close_fd(fd);        // close what we currently own
      fd = other.fd;         // take ownership
      other.fd = invalid_fd; // empty out the source
    }
    return *this;
  }

protected:
  Socket() = default; // only subclasses can construct empty
  explicit Socket(socket_t fd) : fd(fd) {} // only subclasses can wrap an fd
};

// tcp connector class
class TcpConnection : public Socket {
public:
  // constructor — takes an already connected fd
  explicit TcpConnection(socket_t fd);

  // destructor — stops threads, closes fd
  ~TcpConnection();

  // user calls this to set their message handler
  void on_message(std::function<void(std::string_view)> callback);

  // user calls this to queue a message for sending
  void send(std::string msg);

  // starts the sender and receiver threads
  void start();

private:
  // sender thread function
  void sender_loop(std::stop_token stop);

  // receiver thread function
  void receiver_loop(std::stop_token stop);

  std::queue<std::string> outbound;
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  std::function<void(std::string_view)> message_callback;
  std::jthread sender_thread;
  std::jthread receiver_thread;
};

} // namespace ezsock
