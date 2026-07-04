#include "ezSock.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

extern "C" {
#include "miniupnpc.h"
#include "upnpcommands.h"
#include "upnperrors.h"
}

namespace ezsock {

// --- Server Implementation ---

void Server::start_tcp(uint16_t port) {
    tcp_listener = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (tcp_listener == invalid_fd) throw_error("TCP listener creation failed");

    int off = 0;
    ::setsockopt(tcp_listener, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));

    int opt = 1;
    ::setsockopt(tcp_listener, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

    if (::bind(tcp_listener, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        close_fd(tcp_listener);
        throw_error("TCP bind failed");
    }

    if (::listen(tcp_listener, SOMAXCONN) == -1) {
        close_fd(tcp_listener);
        throw_error("TCP listen failed");
    }

    if (!receiver_thread.joinable()) {
        receiver_thread = std::jthread([this](std::stop_token stop) { receiver_loop(stop); });
    }
    if (!sender_thread.joinable()) {
        sender_thread = std::jthread([this](std::stop_token stop) { sender_loop(stop); });
    }
}

void Server::start_udp(uint16_t port) {
    udp_socket = ::socket(AF_INET6, SOCK_DGRAM, 0);
    if (udp_socket == invalid_fd) throw_error("UDP socket creation failed");

    int off = 0;
    ::setsockopt(udp_socket, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));

    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

    if (::bind(udp_socket, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        close_fd(udp_socket);
        throw_error("UDP bind failed");
    }

    if (!receiver_thread.joinable()) {
        receiver_thread = std::jthread([this](std::stop_token stop) { receiver_loop(stop); });
    }
    if (!sender_thread.joinable()) {
        sender_thread = std::jthread([this](std::stop_token stop) { sender_loop(stop); });
    }
}

void Server::on_connect(std::function<void(uint32_t, std::span<std::byte>)> cb) {
    connect_callback = std::move(cb);
}

void Server::on_disconnect(std::function<void(uint32_t)> cb) {
    disconnect_callback = std::move(cb);
}

void Server::register_handler(uint8_t type, std::function<void(uint32_t, std::span<std::byte>)> handler) {
    handlers[type] = std::move(handler);
}

void Server::send_internal(uint32_t id, Protocol proto, uint8_t type, std::span<const std::byte> data) {
    std::vector<std::byte> payload;
    payload.reserve(1 + data.size());
    payload.push_back(static_cast<std::byte>(type));
    payload.insert(payload.end(), data.begin(), data.end());

    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        outbound_queue.push({id, proto, type, std::move(payload)});
    }
    queue_cv.notify_one();
}

void Server::broadcast_internal(Protocol proto, std::span<const uint32_t> exclude, uint8_t type, std::span<const std::byte> data) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    if (proto == Protocol::TCP) {
        for (auto const& [id, fd] : tcp_clients) {
            bool is_excluded = false;
            for (uint32_t ex_id : exclude) {
                if (id == ex_id) { is_excluded = true; break; }
            }
            if (!is_excluded) send_internal(id, proto, type, data);
        }
    } else {
        for (auto const& [id, addr] : udp_clients) {
            bool is_excluded = false;
            for (uint32_t ex_id : exclude) {
                if (id == ex_id) { is_excluded = true; break; }
            }
            if (!is_excluded) send_internal(id, proto, type, data);
        }
    }
}

void Server::receiver_loop(std::stop_token stop) {
    std::vector<std::byte> buffer(4096);
    while (!stop.stop_requested()) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        socket_t max_fd = 0;

        if (tcp_listener != invalid_fd) {
            FD_SET(tcp_listener, &read_fds);
            max_fd = std::max(max_fd, tcp_listener);
        }
        if (udp_socket != invalid_fd) {
            FD_SET(udp_socket, &read_fds);
            max_fd = std::max(max_fd, udp_socket);
        }
        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            for (auto const& [id, fd] : tcp_clients) {
                FD_SET(fd, &read_fds);
                max_fd = std::max(max_fd, fd);
            }
        }
        timeval timeout{0, 100000};
        int activity = select(max_fd + 1, &read_fds, nullptr, nullptr, &timeout);
        if (activity <= 0) continue;

        if (tcp_listener != invalid_fd && FD_ISSET(tcp_listener, &read_fds)) {
            if (!lobby_open) {
                socket_t temp_fd = ::accept(tcp_listener, nullptr, nullptr);
                if (temp_fd != invalid_fd) close_fd(temp_fd);
                continue;
            }
            socket_t client_fd = ::accept(tcp_listener, nullptr, nullptr);
            if (client_fd != invalid_fd) {
                timeval send_tv{0, 100000};
                ::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &send_tv, sizeof(send_tv));
                sockaddr_storage peer{};
                socklen_t peer_len = sizeof(peer);
                if (::getpeername(client_fd, (struct sockaddr*)&peer, &peer_len) == 0) {
                    char ip_str[INET6_ADDRSTRLEN];
                    if (peer.ss_family == AF_INET) {
                        auto *p4 = reinterpret_cast<sockaddr_in*>(&peer);
                        inet_ntop(AF_INET, &p4->sin_addr, ip_str, socklen_t(sizeof(ip_str)));
                    } else {
                        auto *p6 = reinterpret_cast<sockaddr_in6*>(&peer);
                        inet_ntop(AF_INET6, &p6->sin6_addr, ip_str, socklen_t(sizeof(ip_str)));
                    }
                    if (banned_ips.contains(ip_str)) {
                        close_fd(client_fd);
                        continue;
                    }
                }
                uint32_t id = next_id++;
                std::vector<std::byte> metadata_buf(metadata_buffer_size);
                int n = ::recv(client_fd, reinterpret_cast<char*>(metadata_buf.data()), metadata_buffer_size, 0);
                if (n > 0) {
                    metadata_buf.resize(n);
                    {
                        std::lock_guard<std::mutex> lock(clients_mutex);
                        tcp_clients[id] = client_fd;
                    }
                    // Join Ack
                    std::vector<std::byte> ack(5);
                    ack[0] = static_cast<std::byte>(0);
                    std::memcpy(ack.data() + 1, &id, 4);
                    ::send(client_fd, reinterpret_cast<const char*>(ack.data()), 5, 0);
                    if (connect_callback) connect_callback(id, std::span(metadata_buf));
                } else {
                    close_fd(client_fd);
                }
            }
        }

        if (udp_socket != invalid_fd && FD_ISSET(udp_socket, &read_fds)) {
            sockaddr_storage from{};
            socklen_t from_len = sizeof(from);
            int n = ::recvfrom(udp_socket, reinterpret_cast<char*>(buffer.data()), buffer.size(), 0, (struct sockaddr*)&from, &from_len);
            if (n > 0) {
                if (n < 5) continue;
                uint32_t client_id;
                std::memcpy(&client_id, buffer.data(), 4);
                {
                    std::lock_guard<std::mutex> lock(clients_mutex);
                    udp_clients[client_id] = from;
                }
                uint8_t type = static_cast<uint8_t>(buffer[4]);
                std::span<std::byte> payload(buffer.data() + 5, n - 5);
                bool allowed = true;
                for (const auto& v : validators) {
                    if (!v(client_id, type, payload)) { allowed = false; break; }
                }
                if (allowed) {
                    auto it = handlers.find(type);
                    if (it != handlers.end()) it->second(client_id, payload);
                }
            }
        }

        {
            std::vector<std::pair<uint32_t, std::vector<std::byte>>> pending;
            std::vector<uint32_t> disconnected;
            {
                std::lock_guard<std::mutex> lock(clients_mutex);
                for (auto it = tcp_clients.begin(); it != tcp_clients.end(); ) {
                    uint32_t id = it->first;
                    socket_t fd = it->second;
                    if (FD_ISSET(fd, &read_fds)) {
                        int n = ::recv(fd, reinterpret_cast<char*>(buffer.data()), buffer.size(), 0);
                        if (n <= 0) {
                            close_fd(fd);
                            udp_clients.erase(id);
                            it = tcp_clients.erase(it);
                            disconnected.push_back(id);
                            continue;
                        }
                        if (n > 0) {
                            pending.emplace_back(id, std::vector<std::byte>(buffer.begin(), buffer.begin() + n));
                        }
                    }
                    ++it;
                }
            }
            for (uint32_t id : disconnected) {
                if (disconnect_callback) disconnect_callback(id);
            }
            for (auto& [id, data] : pending) {
                uint8_t type = static_cast<uint8_t>(data[0]);
                std::span<std::byte> payload(data.data() + 1, data.size() - 1);
                bool allowed = true;
                for (const auto& v : validators) {
                    if (!v(id, type, payload)) { allowed = false; break; }
                }
                if (allowed) {
                    auto hit = handlers.find(type);
                    if (hit != handlers.end()) hit->second(id, payload);
                }
            }
        }
    }
}

void Server::sender_loop(std::stop_token stop) {
    while (!stop.stop_requested()) {
        Packet packet;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this, &stop] {
                return !outbound_queue.empty() || stop.stop_requested();
            });
            if (stop.stop_requested() && outbound_queue.empty()) break;
            packet = std::move(outbound_queue.front());
            outbound_queue.pop();
        }
        if (packet.protocol == Protocol::TCP) {
            std::lock_guard<std::mutex> lock(clients_mutex);
            if (tcp_clients.contains(packet.target_id)) {
                socket_t fd = tcp_clients[packet.target_id];
                ::send(fd, reinterpret_cast<const char*>(packet.data.data()), packet.data.size(), 0);
            }
        } else {
            std::lock_guard<std::mutex> lock(clients_mutex);
            if (udp_clients.contains(packet.target_id)) {
                sockaddr_storage addr = udp_clients[packet.target_id];
                ::sendto(udp_socket, reinterpret_cast<const char*>(packet.data.data()), 
                        packet.data.size(), 0, (struct sockaddr*)&addr, sizeof(addr));
            }
        }
    }
}

void Server::kick(uint32_t id) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    if (tcp_clients.contains(id)) {
        close_fd(tcp_clients[id]);
        tcp_clients.erase(id);
        if (disconnect_callback) disconnect_callback(id);
    }
}

std::string Server::ban(uint32_t id) {
    std::string ip = "unknown";
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        if (tcp_clients.contains(id)) {
            socket_t fd = tcp_clients[id];
            sockaddr_storage peer{};
            socklen_t peer_len = sizeof(peer);
            if (::getpeername(fd, (struct sockaddr*)&peer, &peer_len) == 0) {
                char ip_str[INET6_ADDRSTRLEN];
                if (peer.ss_family == AF_INET) {
                    auto *p4 = reinterpret_cast<sockaddr_in*>(&peer);
                    inet_ntop(AF_INET, &p4->sin_addr, ip_str, socklen_t(sizeof(ip_str)));
                } else {
                    auto *p6 = reinterpret_cast<sockaddr_in6*>(&peer);
                    inet_ntop(AF_INET6, &p6->sin6_addr, ip_str, socklen_t(sizeof(ip_str)));
                }
                ip = ip_str;
            }
        }
    }
    if (ip != "unknown") banned_ips.insert(ip);
    kick(id);
    return ip;
}

void Server::unban_ip(std::string ip) {
    banned_ips.erase(ip);
}

// --- Client Implementation ---

void Client::join(std::string_view tcp_addr, std::string_view udp_addr, std::span<const std::byte> metadata) {
    std::string t_ip, u_ip;
    uint16_t t_port, u_port;

    auto parse = [](std::string_view addr, std::string& ip, uint16_t& port) {
        if (addr.front() == '[') {
            auto close = addr.find(']');
            if (close == std::string_view::npos)
                throw std::runtime_error("Invalid IPv6 address (missing ']')");
            ip = addr.substr(1, close - 1);
            if (close + 1 >= addr.size() || addr[close + 1] != ':')
                throw std::runtime_error("Invalid IPv6 address (missing port after ']:')");
            port = static_cast<uint16_t>(std::stoi(std::string(addr.substr(close + 2))));
        } else {
            auto colon = addr.rfind(':');
            if (colon == std::string_view::npos || colon == 0)
                throw std::runtime_error("Invalid address");
            ip = addr.substr(0, colon);
            port = static_cast<uint16_t>(std::stoi(std::string(addr.substr(colon + 1))));
        }
    };
    parse(tcp_addr, t_ip, t_port);
    parse(udp_addr, u_ip, u_port);

    auto detect_family = [](const std::string& ip) -> int {
        unsigned char buf[sizeof(in6_addr)];
        if (inet_pton(AF_INET6, ip.c_str(), buf) == 1) return AF_INET6;
        if (inet_pton(AF_INET, ip.c_str(), buf) == 1) return AF_INET;
        return AF_UNSPEC;
    };

    auto setup_addr = [](sockaddr_storage& addr, socklen_t& len,
                          const std::string& ip, uint16_t port, int family) {
        if (family == AF_INET6) {
            addr.ss_family = AF_INET6;
            auto *a6 = reinterpret_cast<sockaddr_in6*>(&addr);
            a6->sin6_port = htons(port);
            inet_pton(AF_INET6, ip.c_str(), &a6->sin6_addr);
            len = sizeof(sockaddr_in6);
        } else {
            addr.ss_family = AF_INET;
            auto *a4 = reinterpret_cast<sockaddr_in*>(&addr);
            a4->sin_port = htons(port);
            inet_pton(AF_INET, ip.c_str(), &a4->sin_addr);
            len = sizeof(sockaddr_in);
        }
    };

    // --- TCP connect ---
    int tcp_family = detect_family(t_ip);
    if (tcp_family == AF_UNSPEC)
        throw std::runtime_error("Invalid TCP server IP: " + t_ip);

    tcp_sock = ::socket(tcp_family, SOCK_STREAM, 0);
    if (tcp_sock == invalid_fd) throw_error("TCP socket creation failed");

    sockaddr_storage t_peer{};
    socklen_t t_peer_len = 0;
    setup_addr(t_peer, t_peer_len, t_ip, t_port, tcp_family);
    if (::connect(tcp_sock, (struct sockaddr*)&t_peer, t_peer_len) == -1)
        throw_error("TCP connect failed");

    if (!metadata.empty()) {
        ::send(tcp_sock, reinterpret_cast<const char*>(metadata.data()), metadata.size(), 0);
    }

    // --- UDP socket ---
    int udp_family = detect_family(u_ip);
    if (udp_family == AF_UNSPEC)
        throw std::runtime_error("Invalid UDP server IP: " + u_ip);

    udp_sock = ::socket(udp_family, SOCK_DGRAM, 0);
    if (udp_sock == invalid_fd) throw_error("UDP socket creation failed");

    sockaddr_storage udp_bind{};
    socklen_t udp_bind_len = 0;
    if (udp_family == AF_INET6) {
        udp_bind.ss_family = AF_INET6;
        reinterpret_cast<sockaddr_in6*>(&udp_bind)->sin6_addr = in6addr_any;
        reinterpret_cast<sockaddr_in6*>(&udp_bind)->sin6_port = htons(0);
        udp_bind_len = sizeof(sockaddr_in6);
    } else {
        udp_bind.ss_family = AF_INET;
        reinterpret_cast<sockaddr_in*>(&udp_bind)->sin_addr.s_addr = INADDR_ANY;
        reinterpret_cast<sockaddr_in*>(&udp_bind)->sin_port = htons(0);
        udp_bind_len = sizeof(sockaddr_in);
    }
    if (::bind(udp_sock, (struct sockaddr*)&udp_bind, udp_bind_len) == -1) {
        close_fd(udp_sock);
        throw_error("UDP bind failed");
    }

    setup_addr(server_udp_addr, server_udp_addr_len, u_ip, u_port, udp_family);
}

void Client::send_internal(Protocol proto, uint8_t type, std::span<const std::byte> data) {
    std::vector<std::byte> payload;
    payload.reserve(1 + data.size());
    payload.push_back(static_cast<std::byte>(type));
    payload.insert(payload.end(), data.begin(), data.end());
    if (proto == Protocol::TCP) {
        ::send(tcp_sock, reinterpret_cast<const char*>(payload.data()), payload.size(), 0);
    } else {
        std::vector<std::byte> udp_packet;
        udp_packet.reserve(4 + payload.size());
        std::byte id_bytes[4];
        std::memcpy(id_bytes, &client_id, 4);
        udp_packet.insert(udp_packet.end(), id_bytes, id_bytes + 4);
        udp_packet.insert(udp_packet.end(), payload.begin(), payload.end());
        ::sendto(udp_sock, reinterpret_cast<const char*>(udp_packet.data()), udp_packet.size(), 0, 
                (struct sockaddr*)&server_udp_addr, server_udp_addr_len);
    }
}

void Client::register_handler(uint8_t type, std::function<void(std::span<std::byte>)> handler) {
    handlers[type] = std::move(handler);
}

void Client::on_disconnect(std::function<void()> cb) {
    disconnect_callback = std::move(cb);
}

void Client::start(size_t buffer_size) {
    receiver_thread = std::jthread([this, buffer_size](std::stop_token stop) {
        std::vector<std::byte> buf(buffer_size);
        while (!stop.stop_requested()) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            socket_t max_fd = 0;
            FD_SET(tcp_sock, &read_fds);
            max_fd = std::max(max_fd, tcp_sock);
            FD_SET(udp_sock, &read_fds);
            max_fd = std::max(max_fd, udp_sock);

            timeval timeout{0, 100000};
            int activity = select(static_cast<int>(max_fd + 1), &read_fds, nullptr, nullptr, &timeout);
            if (activity <= 0) continue;

            if (FD_ISSET(tcp_sock, &read_fds)) {
                int n = ::recv(tcp_sock, reinterpret_cast<char*>(buf.data()), buf.size(), 0);
                if (n <= 0) {
                    if (disconnect_callback) disconnect_callback();
                    return;
                }
                uint8_t type = static_cast<uint8_t>(buf[0]);
                if (type == 0 && n >= 5) {
                    std::memcpy(&client_id, buf.data() + 1, 4);
                } else {
                    std::span<std::byte> payload(buf.data() + 1, n - 1);
                    if (handlers.contains(type)) handlers[type](payload);
                }
            }

            if (FD_ISSET(udp_sock, &read_fds)) {
                int n = ::recv(udp_sock, reinterpret_cast<char*>(buf.data()), buf.size(), 0);
                if (n > 0) {
                    uint8_t type = static_cast<uint8_t>(buf[0]);
                    std::span<std::byte> payload(buf.data() + 1, n - 1);
                    if (handlers.contains(type)) handlers[type](payload);
                }
            }
        }
    });
}

// ============================================================
// UPnP Implementation
// ============================================================
struct UPnP::Impl {
    UPNPUrls urls = {};
    IGDdatas data = {};
    char lan_addr[64] = {};
    int discovery_delay = 2000;
};

UPnP::UPnP(std::initializer_list<PortMapping> mappings)
    : impl_(std::make_unique<Impl>())
    , mappings_(mappings)
{
    UPNPDev *devlist = upnpDiscover(impl_->discovery_delay, nullptr, nullptr, 0, 0, 2, nullptr);
    if (!devlist)
        throw std::runtime_error("UPnP: No IGD device found (discovery failed)");

    int igd = UPNP_GetValidIGD(devlist, &impl_->urls, &impl_->data,
                                impl_->lan_addr, sizeof(impl_->lan_addr),
                                nullptr, 0);
    freeUPNPDevlist(devlist);

    if (igd != 1)
        throw std::runtime_error("UPnP: No valid Internet Gateway Device found");

    for (auto& m : mappings_) {
        const char *proto = (m.protocol == Protocol::TCP) ? "TCP" : "UDP";
        int r = UPNP_AddPortMapping(impl_->urls.controlURL, impl_->data.first.servicetype,
                                    std::to_string(m.port).c_str(),
                                    std::to_string(m.port).c_str(),
                                    impl_->lan_addr,
                                    m.description.c_str(),
                                    proto, nullptr, nullptr);
        if (r != 0) {
            std::string err = strupnperror(r);
            throw std::runtime_error("UPnP: Failed to forward port " +
                                     std::to_string(m.port) + "/" + proto + ": " + err);
        }
    }
}

UPnP::~UPnP() {
    if (!impl_) return;
    for (auto& m : mappings_) {
        const char *proto = (m.protocol == Protocol::TCP) ? "TCP" : "UDP";
        UPNP_DeletePortMapping(impl_->urls.controlURL, impl_->data.first.servicetype,
                               std::to_string(m.port).c_str(), proto, nullptr);
    }
    FreeUPNPUrls(&impl_->urls);
}

UPnP::UPnP(UPnP&&) noexcept = default;
UPnP& UPnP::operator=(UPnP&&) noexcept = default;

// ============================================================
// get_local_ip
// ============================================================
std::string get_local_ip() {
#ifdef _WIN32
    std::string ip = "127.0.0.1";
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;
    addrinfo *result = nullptr;
    if (getaddrinfo(nullptr, nullptr, &hints, &result) != 0)
        return ip;
    for (addrinfo *rp = result; rp; rp = rp->ai_next) {
        char buf[INET6_ADDRSTRLEN];
        if (rp->ai_family == AF_INET) {
            auto *sa = reinterpret_cast<sockaddr_in*>(rp->ai_addr);
            inet_ntop(AF_INET, &sa->sin_addr, buf, socklen_t(sizeof(buf)));
            if (std::strcmp(buf, "127.0.0.1") != 0) {
                ip = buf; break;
            }
        }
    }
    if (ip == "127.0.0.1") {
        for (addrinfo *rp = result; rp; rp = rp->ai_next) {
            char buf[INET6_ADDRSTRLEN];
            if (rp->ai_family == AF_INET6) {
                auto *sa6 = reinterpret_cast<sockaddr_in6*>(rp->ai_addr);
                inet_ntop(AF_INET6, &sa6->sin6_addr, buf, socklen_t(sizeof(buf)));
                if (std::strcmp(buf, "::1") != 0) {
                    ip = buf; break;
                }
            }
        }
    }
    freeaddrinfo(result);
    return ip;
#else
    std::string ip = "127.0.0.1";
    ifaddrs *ifaddr = nullptr;
    if (getifaddrs(&ifaddr) == -1) return ip;
    for (ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET)
            continue;
        if (std::strcmp(ifa->ifa_name, "lo") == 0)
            continue;
        auto *sa = reinterpret_cast<sockaddr_in*>(ifa->ifa_addr);
        char buf[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET, &sa->sin_addr, buf, socklen_t(sizeof(buf)));
        if (std::strcmp(buf, "127.0.0.1") != 0) {
            ip = buf; break;
        }
    }
    if (ip == "127.0.0.1") {
        for (ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
            if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET6)
                continue;
            if (std::strcmp(ifa->ifa_name, "lo") == 0)
                continue;
            auto *sa6 = reinterpret_cast<sockaddr_in6*>(ifa->ifa_addr);
            char buf[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, &sa6->sin6_addr, buf, socklen_t(sizeof(buf)));
            if (std::strcmp(buf, "::1") != 0) {
                ip = buf; break;
            }
        }
    }
    freeifaddrs(ifaddr);
    return ip;
#endif
}

} // namespace ezsock
