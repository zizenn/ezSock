#include "ezSock.hpp"
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace ezsock;

// ============================================================
// Protocol
// ============================================================
enum MessageType : uint8_t {
    MSG_CHAT       = 1,
    MSG_POSITION   = 2,
    MSG_SYSTEM     = 3,
    MSG_PLAYER_LIST = 4,
};

struct Position {
    float x, y, z;
};

struct PosUpdate {
    uint32_t sender_id;
    Position pos;
};

// ============================================================
// SERVER
// ============================================================
void run_server() {
    std::unordered_map<uint32_t, std::string> players;
    std::mutex players_mtx;

    std::unordered_map<uint32_t, std::chrono::steady_clock::time_point> last_msg;
    std::mutex rate_mtx;

    Server server;
    server.add_validator([&](uint32_t id, uint8_t, std::span<std::byte>) -> bool {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(rate_mtx);
        auto it = last_msg.find(id);
        if (it != last_msg.end()) {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();
            if (ms < 10) return false;
        }
        last_msg[id] = now;
        return true;
    });

    // --- Connect ---
    server.on_connect([&](uint32_t id, std::span<std::byte> meta) {
        std::string name(reinterpret_cast<const char*>(meta.data()), meta.size());
        {
            std::lock_guard<std::mutex> lock(players_mtx);
            players[id] = name;
        }
        std::cout << "[+] " << name << " (#" << id << ") joined" << std::endl;
        server.send_tcp(id, MSG_SYSTEM, "Welcome! Your ID is " + std::to_string(id));
        server.broadcast_except_tcp(std::vector<uint32_t>{id}, MSG_SYSTEM, name + " joined the game!");
    });

    // --- Disconnect ---
    server.on_disconnect([&](uint32_t id) {
        std::string name = "Unknown";
        {
            std::lock_guard<std::mutex> lock(players_mtx);
            if (players.contains(id)) {
                name = players[id];
                players.erase(id);
            }
        }
        std::cout << "[-] " << name << " (#" << id << ") left" << std::endl;
        server.broadcast_tcp(MSG_SYSTEM, name + " left the game.");
    });

    // --- Chat handler (TCP) ---
    server.register_handler(MSG_CHAT, [&](uint32_t id, std::span<std::byte> data) {
        std::string msg(reinterpret_cast<const char*>(data.data()), data.size());
        std::string sender;
        {
            std::lock_guard<std::mutex> lock(players_mtx);
            if (players.contains(id)) sender = players[id];
        }

        // Admin commands
        if (!msg.empty() && msg[0] == '/') {
            std::istringstream iss(msg);
            std::string cmd;
            iss >> cmd;
            std::string rest;
            std::getline(iss >> std::ws, rest);

            if (cmd == "/kick" && !rest.empty()) {
                uint32_t target = std::stoul(rest);
                server.broadcast_tcp(MSG_SYSTEM, sender + " kicked #" + std::to_string(target));
                server.kick(target);
            } else if (cmd == "/ban" && !rest.empty()) {
                uint32_t target = std::stoul(rest);
                std::string ip = server.ban(target);
                server.broadcast_tcp(MSG_SYSTEM, sender + " banned #" + std::to_string(target) + " (" + ip + ")");
            } else if (cmd == "/unban" && !rest.empty()) {
                server.unban_ip(rest);
                server.broadcast_tcp(MSG_SYSTEM, "Unbanned " + rest);
            } else if (cmd == "/lobby") {
                server.set_lobby_open(!server.is_lobby_open());
                server.broadcast_tcp(MSG_SYSTEM,
                    std::string("Lobby ") + (server.is_lobby_open() ? "open" : "closed"));
            } else if (cmd == "/players") {
                std::string list;
                {
                    std::lock_guard<std::mutex> lock(players_mtx);
                    for (auto const& [pid, pname] : players)
                        list += "  #" + std::to_string(pid) + " " + pname + "\n";
                }
                server.send_tcp(id, MSG_SYSTEM, "Players online:\n" + list);
            }
            return;
        }

        std::cout << "[Chat] " << sender << ": " << msg << std::endl;
        server.broadcast_except_tcp(std::vector<uint32_t>{id}, MSG_CHAT, sender + ": " + msg);
    });

    // --- Position handler (UDP) ---
    server.register_handler(MSG_POSITION, [&](uint32_t id, std::span<std::byte> data) {
        if (data.size() < sizeof(Position)) return;
        Position pos;
        std::memcpy(&pos, data.data(), sizeof(Position));
        PosUpdate upd{id, pos};
        server.broadcast_except_udp(std::vector<uint32_t>{id}, MSG_POSITION, upd);
    });

    // --- Start ---
    server.start_tcp(8080);
    server.start_udp(8081);
    server.set_metadata_buffer_size(256);

    std::cout << "\n=== ezSock Game Server ===" << std::endl;
    std::cout << "TCP :8080 | UDP :8081" << std::endl;
    std::cout << "Admin: /kick <id> /ban <id> /unban <ip> /lobby /players" << std::endl;
    std::cout << "Console: 'quit' to stop, 'players' to list, 'lobby' to toggle\n" << std::endl;

    std::string input;
    while (std::getline(std::cin, input)) {
        if (input == "quit") break;
        if (input == "players") {
            std::lock_guard<std::mutex> lock(players_mtx);
            std::cout << players.size() << " online:" << std::endl;
            for (auto const& [pid, pname] : players)
                std::cout << "  #" << pid << " " << pname << std::endl;
        }
        if (input == "lobby") {
            server.set_lobby_open(!server.is_lobby_open());
            std::cout << "Lobby " << (server.is_lobby_open() ? "opened" : "closed") << std::endl;
        }
    }
}

// ============================================================
// CLIENT
// ============================================================
void run_client(std::string name) {
    std::atomic<bool> connected{true};
    Client client;

    client.register_handler(MSG_CHAT, [](std::span<std::byte> data) {
        std::string msg(reinterpret_cast<const char*>(data.data()), data.size());
        std::cout << "\r" << msg << "\n>> " << std::flush;
    });

    client.register_handler(MSG_SYSTEM, [](std::span<std::byte> data) {
        std::string msg(reinterpret_cast<const char*>(data.data()), data.size());
        std::cout << "\r[SYS] " << msg << "\n>> " << std::flush;
    });

    client.register_handler(MSG_PLAYER_LIST, [](std::span<std::byte> data) {
        std::string msg(reinterpret_cast<const char*>(data.data()), data.size());
        std::cout << "\r[Players]\n" << msg << ">> " << std::flush;
    });

    client.register_handler(MSG_POSITION, [](std::span<std::byte> data) {
        if (data.size() < sizeof(PosUpdate)) return;
        PosUpdate upd;
        std::memcpy(&upd, data.data(), sizeof(PosUpdate));
    });

    client.on_disconnect([&]() {
        std::cout << "\r[!] Disconnected from server\n" << std::flush;
        connected = false;
    });

    try {
        std::vector<std::byte> meta;
        for (char c : name) meta.push_back(static_cast<std::byte>(c));

        client.join("127.0.0.1:8080", "127.0.0.1:8081", meta);
        client.start();

        std::cout << "Connected as " << name << "!\n>> " << std::flush;

        std::atomic<bool> running{true};
        std::mt19937 rng(std::random_device{}());
        std::uniform_real_distribution<float> dist(-5.0f, 5.0f);

        std::thread pos_thread([&]() {
            while (running && connected) {
                std::this_thread::sleep_for(std::chrono::seconds(2));
                if (!running || !connected) break;
                Position p{dist(rng), dist(rng), dist(rng)};
                client.send_udp(MSG_POSITION, p);
            }
        });

        std::string input;
        while (connected && std::getline(std::cin, input)) {
            if (input == "/quit") break;
            if (input.empty()) {
                std::cout << ">> " << std::flush;
                continue;
            }
            if (input == "/pos") {
                Position p{1.0f, 2.0f, 3.0f};
                client.send_udp(MSG_POSITION, p);
                std::cout << "[Sent position via UDP]\n>> " << std::flush;
                continue;
            }
            client.send_tcp(MSG_CHAT, input);
            std::cout << ">> " << std::flush;
        }

        running = false;
        if (pos_thread.joinable()) pos_thread.join();

    } catch (std::exception const& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

// ============================================================
// MAIN
// ============================================================
int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Usage:\n"
                  << "  " << argv[0] << " server\n"
                  << "  " << argv[0] << " client <name>\n";
        return 1;
    }
    std::string mode(argv[1]);
    if (mode == "server") {
        run_server();
    } else if (mode == "client" && argc >= 3) {
        run_client(argv[2]);
    } else {
        std::cout << "Invalid arguments" << std::endl;
        return 1;
    }
    return 0;
}
