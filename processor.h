#ifndef Processor_H
#define Processor_H

/************************
    # Increase open file limit
    ulimit -n 65535

    # Or permanently in /etc/security/limits.conf:
    # * soft nofile 65535
    # * hard nofile 65535

    # Kernel tuning
    sysctl -w net.core.somaxconn=65535
    sysctl -w net.ipv4.tcp_max_syn_backlog=65535

 ****************************/

#include <asio.hpp>
#include <iostream>
#include <thread>
#include <vector>
#include <array>
#include <atomic>
#include <queue>
#include <string_view>
#include <mutex>
#include <cstdint>
#include <cstring>

#include "safe_ptr.h"

#include "dl_iso8583.h"
#include "dl_iso8583_defs_1993.h"
#include "dl_output.h"

#define MAX_LEN 4096

using asio::ip::tcp;

namespace YSH
{
    class IsoMsg
    {
    public:
        IsoMsg(std::shared_ptr<DL_ISO8583_HANDLER> iso_handler) : iso_handler(iso_handler)
        {
            DL_ISO8583_MSG_Init(NULL, 0, &raw);
        }
        ~IsoMsg()
        {
            DL_ISO8583_MSG_Free(&raw);
        }
        DL_ISO8583_MSG *get()
        {
            return &raw;
        }
        DL_ISO8583_HANDLER *getHandler()
        {
            return iso_handler.get();
        }

    private:
        DL_ISO8583_MSG raw;
        std::shared_ptr<DL_ISO8583_HANDLER> iso_handler;
    };

    struct MessageTaskState
    {
        std::atomic<bool> is_completed{false};
        std::atomic<bool> is_timed_out{false};
        std::shared_ptr<asio::steady_timer> timer;
    };

    inline uint32_t getStan(DL_ISO8583_MSG* msg)
    {
        DL_UINT8 *stanPtr = nullptr;
        DL_UINT16 stanLen = 0;
        DL_ISO8583_MSG_GetField_Bin(11, msg, &stanPtr, &stanLen);
        uint32_t stan = 0;
        std::from_chars(reinterpret_cast<const char*>(stanPtr), reinterpret_cast<const char*>(stanPtr + stanLen), stan);
        return stan;
    }

    struct Config
    {
        Config(
            bool is_server = true,
            uint32_t port = 3000,
            std::string host = "127.0.0.1",
            uint32_t thread_count = 5,
            uint32_t worker_count = 20,
            uint32_t timeout = 2000,
            bool is_persist_connection = false,
            std::function<std::unique_ptr<IsoMsg>(std::unique_ptr<IsoMsg>)> processor = nullptr,
            std::function<std::unique_ptr<IsoMsg>(std::unique_ptr<IsoMsg>)> timeout_processor = nullptr,
            std::function<void(const asio::error_code &ec, std::unique_ptr<IsoMsg>)> handler = nullptr)
            : is_server(is_server), port(port), thread_count(thread_count), worker_count(worker_count), timeout(timeout), is_persist_connection(is_persist_connection), processor(std::move(processor)), timeout_processor(std::move(timeout_processor)), handler(std::move(handler)) {};
        uint32_t port;
        uint32_t thread_count;
        uint32_t worker_count;
        uint32_t timeout;
        bool is_persist_connection;
        std::function<std::unique_ptr<IsoMsg>(std::unique_ptr<IsoMsg>)> processor;
        std::function<std::unique_ptr<IsoMsg>(std::unique_ptr<IsoMsg>)> timeout_processor;
        std::function<void(const asio::error_code &ec, std::unique_ptr<IsoMsg>)> handler;
        std::string host;
        bool is_server;
    };

    // -------------------------
    // ClientSession
    // -------------------------
    class ClientSession : public std::enable_shared_from_this<ClientSession>
    {
    public:
        explicit ClientSession(asio::io_context &io_context, std::shared_ptr<Config> config);

        ~ClientSession();

        void start();
        void finish(asio::error_code ec, std::unique_ptr<IsoMsg>);
        void send(std::unique_ptr<IsoMsg> response);

    private:
        void do_connect(const tcp::resolver::results_type &endpoints);
        void do_write(std::unique_ptr<IsoMsg> iso_msg);
        void read_header();
        void read_body(uint16_t len);

        tcp::socket socket_;

        asio::io_context &io_context_;
        asio::strand<asio::any_io_executor> strand_;

        std::shared_ptr<DL_ISO8583_HANDLER> iso_handler_;

        sf::safe_ptr<std::map<uint32_t, std::shared_ptr<MessageTaskState>>> message_state_;

        std::shared_ptr<Config> config_;
        bool timeout_flg_;
        std::mutex mutex;
    };

    // -------------------------
    // Session
    // -------------------------
    class Session : public std::enable_shared_from_this<Session>
    {
    public:
        explicit Session(tcp::socket socket, asio::io_context &io_context, asio::thread_pool &worker_pool, std::shared_ptr<Config> config);

        ~Session();

        void start();
        void close();

    private:
        void read_header();
        void read_body(uint16_t len);
        void handle_message(std::shared_ptr<MessageTaskState> state,std::unique_ptr<IsoMsg> iso_msg);
        void send_response(std::unique_ptr<IsoMsg> iso_msg);

        tcp::socket socket_;

        asio::io_context &io_context_;
        asio::thread_pool &worker_pool_;
        asio::strand<asio::any_io_executor> strand_;

        std::shared_ptr<DL_ISO8583_HANDLER> iso_handler_;

        uint8_t header_[2];
        std::shared_ptr<Config> config_;
        bool timeout_flg_;
    };

    // -------------------------
    // Server
    // -------------------------
    class Server
    {
    public:
        Server(asio::io_context &io, std::shared_ptr<Config> config);
        ~Server();

    private:
        void do_accept();

        asio::thread_pool worker_pool_;
        asio::io_context &io_;
        tcp::acceptor acceptor_;
        std::shared_ptr<Config> config_;
    };

    // -------------------------
    // Client
    // -------------------------
    class Client
    {
    public:
        Client(asio::io_context &io, std::shared_ptr<Config> config);
        ~Client();
        void stop();
        void send(std::unique_ptr<IsoMsg> request);

    private:
        asio::thread_pool worker_pool_;
        asio::io_context &io_;
        std::shared_ptr<Config> config_;
        std::shared_ptr<ClientSession> client_session_;

        using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;

        WorkGuard workGuard_;
        std::atomic_bool stopped_;
    };

    // -------------------------
    // Processor
    // -------------------------
    class Processor
    {
    public:
        Processor();
        void start(std::shared_ptr<Config> config, bool wait_flg = true);
        void stop();
        void send(std::unique_ptr<IsoMsg> request);

    private:
        std::vector<std::thread> threads_;
        std::shared_ptr<Config> config_;
        asio::io_context io_;
        std::unique_ptr<Server> server_;
        std::unique_ptr<Client> client_;
    };

}

#endif // Processor_H