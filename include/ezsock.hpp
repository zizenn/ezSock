#pragma once

#include "ezSock.hpp"
#include <cstdint>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using socket_t = SOCKET;
using ssize_t = ptrdiff_t; // Standardizes POSIX return type for raw socket
                           // reads on Windows
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
#endif

#include <system_error>

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

// packet system
struct Packet {
      uint8_t type;
      uint32_t length;
};

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

} // namespace ezsock
