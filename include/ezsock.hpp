#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using socket_t = SOCKET;
using ssize_t = std::ptrdiff_t;
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/select.h>
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

class Socket {
private:
      socket_t fd_ = invalid_fd;
      bool nonBlocking_ = false;

      explicit Socket(socket_t fd) : fd_(fd) {}

public:
      Socket() = default;          // constructor
      ~Socket() { close_fd(fd_); } // destructor

      // move operations
      Socket(Socket &&other) noexcept;
      Socket &operator=(Socket &&other) noexcept;

      // deleted copy operations
      Socket(const Socket &) = delete;
      Socket &operator=(const Socket &) = delete;

      // methods
      // queries
      bool IsValid() const { return fd_ != invalid_fd; }
      socket_t NativeHandle() const { return fd_; }
      bool IsNonBlocking() const { return nonBlocking_; }

      // functions
      void SetNonBlocking(bool nonBlocking) { nonBlocking_ = nonBlocking; }
      void Close();
};

} // namespace ezsock
