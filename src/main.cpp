#include "ezSock.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <map>

using namespace ezsock;

// --- Chat Protocol ---
enum MessageType : uint8_t {
    MSG_CHAT = 1,
    MSG_SYSTEM = 2
};

// We use this to track names on the server
std::unordered_map<uint32_t, std::string> player_names;
std::mutex name_mutex;

void run_server() {
    std::cout << "--- ezSock Chat Server ---" << std::endl;
    Server server;

    // Handle Chat: Relay it to everyone else
    server.register_handler(MSG_CHAT, [&](uint32_t id, std::span<std::byte> data) {
        std::string msg(reinterpret_cast<const char*>(data.data()), data.size());
        
        std::string sender_name = "Unknown";
        {
            std::lock_guard<std::mutex> lock(name_mutex);
            if (player_names.contains(id)) sender_name = player_names[id];
        }

        std::string formatted = "[" + sender_name + "]: " + msg;
        std::cout << "[Log] " << formatted << std::endl;
        
        // RELAY: Send to everyone except the sender
        std::vector<uint32_t> exclude = { id };
        server.broadcast_except_tcp(exclude, MSG_CHAT, formatted);
    });

    server.on_connect([](uint32_t id, std::span<std::byte> metadata) {
        std::string name(reinterpret_cast<const char*>(metadata.data()), metadata.size());
        {
            std::lock_guard<std::mutex> lock(name_mutex);
            player_names[id] = name;
        }
        std::cout << " >> " << name << " joined the chat (ID: " << id << ")" << std::endl;
    });

    server.on_disconnect([](uint32_t id) {
        std::cout << " >> Player " << id << " left the chat." << std::endl;
    });

    server.start_tcp(8080);
    server.start_udp(8081);

    std::cout << "Server running on 8080/8081. Waiting for players..." << std::endl;
    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
}

void run_client(std::string name) {
    Client client;

    // Handle incoming messages
    client.register_handler(MSG_CHAT, [](std::span<std::byte> data) {
        std::string msg(reinterpret_cast<const char*>(data.data()), data.size());
        //  clears the current line so the prompt stays at the bottom
        std::cout << "" << msg << "
>> " << std::flush;
    });

    std::string name_str = name;
    std::vector<std::byte> metadata;
    for (char c : name_str) metadata.push_back(static_cast<std::byte>(c));
    
    try {
        client.join("127.0.0.1:8080", "127.0.0.1:8081", metadata);
        client.start();
        
        std::cout << "Connected to chat as " << name << "!" << std::endl;
        std::cout << "Type your message and hit Enter. Type /quit to exit." << std::endl;
        std::cout << ">> " << std::flush;

        std::string input;
        while (std::getline(std::cin, input)) {
            if (input == "/quit") break;
            if (input.empty()) continue;

            client.send_tcp(MSG_CHAT, input);
            std::cout << ">> " << std::flush;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " [server|client <name>]" << std::endl;
        return 1;
    }

    if (std::string(argv[1]) == "server") {
        run_server();
    } else if (std::string(argv[1]) == "client" && argc >= 3) {
        run_client(argv[2]);
    } else {
        std::cout << "Invalid arguments." << std::endl;
    }
    return 0;
}
