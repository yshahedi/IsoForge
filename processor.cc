
#include "processor.h"
#include <format>

using asio::ip::tcp;

namespace Processor
{
    // -------------------------
    // Session
    // -------------------------

    Session::Session(tcp::socket socket, asio::io_context &io_context, asio::thread_pool &worker_pool, std::shared_ptr<Config> config)
        : socket_(std::move(socket)), io_context_(io_context), worker_pool_(worker_pool),config_(config),timer_(socket_.get_executor()) 
    {
        DL_ISO8583_DEFS_1993_GetHandler(&iso_handler_);
    }

    Session::~Session()
    {
    }

    void Session::start()
    {
        read_header();
    }

    void Session::close()
    {
        /*socket_.shutdown(tcp::socket::shutdown_both);
        socket_.close();*/
    }

    void Session::read_header()
    {
        auto self = shared_from_this();
        asio::async_read(socket_, asio::buffer(header_, 2),
                         [this, self](std::error_code ec, std::size_t)
                         {
                             if (ec)
                                 return close();
                             uint16_t len = (static_cast<uint16_t>(header_[0]) << 8) | header_[1];
                             if (len == 0 || len > MAX_LEN)
                             {
                                 return close();
                             }

                             read_body(len);
                         });
    }

    void Session::read_body(uint16_t len)
    {
        auto self = shared_from_this();
        
        asio::async_read(socket_, asio::buffer(asio::buffer(body_, len)),
                         [this, self, len](std::error_code ec, std::size_t length)
                         {
                             if (ec)
                                 return close();
                             body_len_ = length;
                             set_timeout(std::chrono::milliseconds(config_->timeout));
                             if (!config_->is_persist_connection)
                             {
                                 asio::post(self->worker_pool_, [self, this]()
                                            { self->handle_message(body_, body_len_); });
                             }
                             else
                             {
                                 self->handle_message(body_, body_len_);
                                 read_header();
                             }
                         });
    }

    void Session::handle_message(const uint8_t *data, size_t len)
    {
        DL_ISO8583_MSG_Init(NULL, 0, &iso_msg);

        (void)DL_ISO8583_MSG_Unpack(&iso_handler_, data, len, &iso_msg);

        config_->processor(iso_handler_, iso_msg);
        {
            timer_.cancel();
            std::lock_guard<std::mutex> lock(mtx_);
            if(!timeout_flg_)
            {
                timeout_flg_=true;
                DL_UINT8 packBuf[MAX_LEN];
                DL_UINT16 packedSize;

                (void)DL_ISO8583_MSG_Pack(&iso_handler_, &iso_msg, packBuf, &packedSize);

                asio::post(io_context_, [self = shared_from_this(), packBuf, packedSize]()
                        { self->send_response(packBuf, packedSize); });
            }
            DL_ISO8583_MSG_Free(&iso_msg);
        }
    }

    void Session::send_response(const uint8_t *data, size_t len)
    {
        auto self = shared_from_this();
        asio::async_write(socket_, asio::buffer(data, len),
                          [this, self, data, len](std::error_code ec, std::size_t)
                          {
                              if (ec)
                                  return;
                          });
    }

    void Session::set_timeout(std::chrono::milliseconds dur)
    {
        timeout_flg_=false;
        timer_.expires_after(dur);
        timer_.async_wait([this,self = shared_from_this()](asio::error_code ec)
                          {
            if (!ec) 
            {
                std::lock_guard<std::mutex> lock(mtx_);
                if(timeout_flg_) return;
                timeout_flg_=true;
                DL_ISO8583_MSG_Init(NULL, 0, &iso_timeout_msg);

                (void)DL_ISO8583_MSG_Unpack(&iso_handler_, body_, body_len_, &iso_timeout_msg);
                config_->timeout_processor(iso_handler_, iso_timeout_msg);

                DL_UINT8 packBuf[MAX_LEN];
                DL_UINT16 packedSize;

                (void)DL_ISO8583_MSG_Pack(&iso_handler_, &iso_timeout_msg, packBuf, &packedSize);

                asio::post(io_context_, [self = shared_from_this(), packBuf, packedSize]()
                        { self->send_response(packBuf, packedSize); });

                DL_ISO8583_MSG_Free(&iso_timeout_msg);
            } 
        });
    }

    // -------------------------
    // Server
    // -------------------------

    Server::Server(asio::io_context &io, std::shared_ptr<Config> config)
        : io_(io), acceptor_(io, tcp::endpoint(tcp::v4(), config->port)), worker_pool_(config->worker_count), config_(config)
    {
        acceptor_.listen(65535);
        do_accept();
    }

    void Server::do_accept()
    {
        acceptor_.async_accept(
            [this](std::error_code ec, tcp::socket socket)
            {
                if (!ec)
                {
                    std::make_shared<Session>(std::move(socket), io_, worker_pool_, config_)->start();
                }
                do_accept();
            });
    }

    // -------------------------
    // Processor
    // -------------------------
    Processor::Processor()
    {
    }
    void Processor::stop()
    {
    }
    void Processor::start(std::shared_ptr<Config> config)
    {
        config_ = config;
        try
        {

            Server server(io_, config_);
            threads_.reserve(config_->thread_count);
            for (int i = 0; i < config_->thread_count; ++i)
            {
                threads_.emplace_back([&]()
                                      { io_.run(); });
            }

            for (auto &t : threads_)
                t.join();
        }
        catch (const std::exception &e)
        {
            std::cerr << "Fatal: " << e.what() << std::endl;
        }
        
    }

   
}


/*const char *create_table = R"(CREATE TABLE card_profiles (
                                        card_number INTEGER PRIMARY KEY,
                                        balance REAL,
                                        last_lat REAL,
                                        last_lon REAL,
                                        last_tx_time INTEGER,
                                        status_code INTEGER,
                                        daily_total REAL
                                    ) WITHOUT ROWID;)";
        for(int i=0;i<=999;i++)
        {
            std::string number = std::format("{:03}", i);
            std::string uri = "file:card"+number+"?mode=memory&cache=shared";
            create_db(number.c_str(),uri.c_str(),create_table);
        }*/