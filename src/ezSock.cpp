#include "ezSock.hpp"
#include <iostream>
#include <algorithm>

namespace ezsock {

// --- Server Implementation ---

void Server::start_tcp(uint16_t port) {
    tcp_listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_listener == invalid_fd) throw_error("TCP listener creation failed");

    int opt = 1;
    ::setsockopt(tcp_listener, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

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
}

void Server::start_udp(uint16_t port) {
    udp_socket = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_socket == invalid_fd) throw_error("UDP socket creation failed");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(udp_socket, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        close_fd(udp_socket);
        throw_error("UDP bind failed");
    }

    if (!receiver_thread.joinable()) {
        receiver_thread = std::jthread([this](std::stop_token stop) { receiver_loop(stop); });
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
                sockaddr_in addr{};
                socklen_t addr_len = sizeof(addr);
                if (::getpeername(client_fd, (struct sockaddr*)&addr, &addr_len) == 0) {
                    char ip_str[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &addr.sin_addr, ip_str, INET_ADDRSTRLEN);
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
            sockaddr_in from{};
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
            std::lock_guard<std::mutex> lock(clients_mutex);
            for (auto it = tcp_clients.begin(); it != tcp_clients.end(); ) {
                uint32_t id = it->first;
                socket_t fd = it->second;
                if (FD_ISSET(fd, &read_fds)) {
                    int n = ::recv(fd, reinterpret_cast<char*>(buffer.data()), buffer.size(), 0);
                    if (n <= 0) {
                        close_fd(fd);
                        {
                            std::lock_guard<std::mutex> lock(clients_mutex);
                            udp_clients.erase(id);
                        }
                        if (disconnect_callback) disconnect_callback(id);
                        it = tcp_clients.erase(it);
                        continue;
                    }

                    if (n < 1) continue;
                    uint8_t type = static_cast<uint8_t>(buffer[0]);
                    std::span<std::byte> payload(buffer.data() + 1, n - 1);
                    bool allowed = true;
                    for (const auto& v : validators) {
                        if (!v(id, type, payload)) { allowed = false; break; }
                    }
                    if (allowed) {
                        auto hit = handlers.find(type);
                        if (hit != handlers.end()) hit->second(id, payload);
                    }
                } else {
                    ++it;
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
                sockaddr_in addr = udp_clients[packet.target_id];
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
            sockaddr_in addr{};
            socklen_t addr_len = sizeof(addr);
            if (::getpeername(fd, (struct sockaddr*)&addr, &addr_len) == 0) {
                char ip_str[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &addr.sin_addr, ip_str, INET_ADDRSTRLEN);
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
        size_t colon = addr.find(':');
        if (colon == std::string_view::npos) throw std::runtime_error("Invalid address");
        ip = std::string(addr.substr(0, colon));
        port = static_cast<uint16_t>(std::stoi(std::string(addr.substr(colon + 1))));
    };
    parse(tcp_addr, t_ip, t_port);
    parse(udp_addr, u_ip, u_port);
    tcp_sock = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in t_addr{};
    t_addr.sin_family = AF_INET;
    t_addr.sin_port = htons(t_port);
    inet_pton(AF_INET, t_ip.c_str(), &t_addr.sin_addr);
    if (::connect(tcp_sock, (struct sockaddr*)&t_addr, sizeof(t_addr)) == -1) throw_error("TCP connect failed");
    if (!metadata.empty()) {
        ::send(tcp_sock, reinterpret_cast<const char*>(metadata.data()), metadata.size(), 0);
    }
    udp_sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    server_udp_addr.sin_family = AF_INET;
    server_udp_addr.sin_port = htons(u_port);
    inet_pton(AF_INET, u_ip.c_str(), &server_udp_addr.sin_addr);
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
                (struct sockaddr*)&server_udp_addr, sizeof(server_udp_addr));
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
            int n = ::recv(tcp_sock, reinterpret_cast<char*>(buf.data()), buf.size(), 0);
            if (n <= 0) {
                if (disconnect_callback) disconnect_callback();
                return;
            }
            if (n > 0) {
                uint8_t type = static_cast<uint8_t>(buf[0]);
                if (type == 0 && n >= 5) {
                    std::memcpy(&client_id, buf.data() + 1, 4);
                } else {
                    std::span<std::byte> payload(buf.data() + 1, n - 1);
                    if (handlers.contains(type)) handlers[type](payload);
                }
            }
        }
    });
}

} // namespace ezsock
