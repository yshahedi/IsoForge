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

#include "dl_iso8583.h"
#include "dl_iso8583_defs_1993.h"
#include "dl_output.h"

#define MAX_LEN 4096

using asio::ip::tcp;

namespace Processor
{

    struct Config
    {
        Config(
            uint32_t port=3000,
            uint32_t thread_count=5,
            uint32_t worker_count=20,
            uint32_t timeout=2000,
            bool is_persist_connection=false,
            std::function<void(DL_ISO8583_HANDLER&,DL_ISO8583_MSG&)> processor =[](DL_ISO8583_HANDLER& isoHandler,DL_ISO8583_MSG& isoMsg){},
            std::function<void(DL_ISO8583_HANDLER&,DL_ISO8583_MSG&)> timeout_processor=[](DL_ISO8583_HANDLER& isoHandler,DL_ISO8583_MSG& isoMsg){}
        ):port(port),thread_count(thread_count),worker_count(worker_count),timeout(timeout),is_persist_connection(is_persist_connection),processor(std::move(processor)),timeout_processor(std::move(timeout_processor))
        {

        };
        uint32_t port;
        uint32_t thread_count;
        uint32_t worker_count;
        uint32_t timeout;
        bool is_persist_connection;
        std::function<void(DL_ISO8583_HANDLER&,DL_ISO8583_MSG&)> processor;
        std::function<void(DL_ISO8583_HANDLER&,DL_ISO8583_MSG&)> timeout_processor;
    };

    // -------------------------
    // Session
    // -------------------------
    class Session : public std::enable_shared_from_this<Session>
    {
    public:
        explicit Session(tcp::socket socket,asio::io_context& io_context, asio::thread_pool& worker_pool, std::shared_ptr<Config> config);

        ~Session();

        void start();
        void close();

    private:
        void read_header();
        void read_body(uint16_t len);
        void handle_message(const uint8_t *data, size_t len);
        void send_response(const uint8_t *data, size_t len);
        void set_timeout(std::chrono::milliseconds dur);

        tcp::socket socket_;
        asio::steady_timer timer_;

        asio::io_context& io_context_;
        asio::thread_pool& worker_pool_;

        DL_ISO8583_HANDLER iso_handler_;
        DL_ISO8583_MSG iso_msg;
        DL_ISO8583_MSG iso_timeout_msg;

        uint8_t header_[2];
        uint8_t body_[MAX_LEN];
        size_t body_len_;
        std::shared_ptr<Config> config_;
        bool timeout_flg_;
        std::mutex mtx_;

    };

    // -------------------------
    // Server
    // -------------------------
    class Server
    {
    public:
        Server(asio::io_context &io, std::shared_ptr<Config> config);

    private:
        void do_accept();
        
        asio::thread_pool worker_pool_;
        asio::io_context &io_;
        tcp::acceptor acceptor_;
        std::shared_ptr<Config> config_;
    };

    // -------------------------
    // Processor
    // -------------------------
    class Processor
    {
        public:
            Processor();
            void start(std::shared_ptr<Config> config);
            void stop();
        private:
            std::vector<std::thread> threads_;
            std::shared_ptr<Config> config_;
            asio::io_context io_;
    };

   

}

#endif // Processor_H