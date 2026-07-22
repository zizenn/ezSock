#include "ezsock.hpp"
#include <sys/socket.h>
#include <utility>

namespace ezsock {

void Socket::Close() {
      if (fd_ != invalid_fd) {
            close_fd(fd_);
            fd_ = invalid_fd;
            this->nonBlocking_ = false;
      }
}

Socket::Socket(Socket &&other) noexcept
    : fd_(std::exchange(other.fd_, invalid_fd)),
      nonBlocking_(std::exchange(other.nonBlocking_, false)) {}

void Socket::Bind(const std::string &address, uint16_t port) {
      if (!IsValid()) {
            throw std::runtime_error("Socket is not valid");
      }
}

} // namespace ezsock
