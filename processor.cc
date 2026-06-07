
#include "processor.h"
#include <format>

using asio::ip::tcp;

namespace YSH
{
    // -------------------------
    // ClientSession
    // -------------------------

    ClientSession::ClientSession(asio::io_context &io_context, asio::thread_pool &worker_pool, std::shared_ptr<Config> config)
        : socket_(io_context), io_context_(io_context), worker_pool_(worker_pool), config_(config), timer_(io_context), resolver_(io_context)
    {
        DL_ISO8583_DEFS_1993_GetHandler(&iso_handler_);
    }

    ClientSession::~ClientSession()
    {
    }

    void ClientSession::start()
    {
    }

    void ClientSession::send_async(std::unique_ptr<IsoMsg> request)
    {
        DL_UINT16 packedSize;
        (void)DL_ISO8583_MSG_Pack(&iso_handler_, request->get(), body_, &packedSize);
        body_len_ = packedSize;

        auto self = shared_from_this();
        set_timeout(std::chrono::milliseconds(config_->timeout));
        /* auto endpoints = resolver_.resolve(config_->host,config_->port);
         self->do_connect(endpoints);*/

        resolver_.async_resolve(
            config_->host,
            std::to_string(config_->port),
            [self](const asio::error_code &ec,
                   tcp::resolver::results_type endpoints)
            {
                if (ec)
                {
                    self->finish(ec, nullptr);
                    return;
                }

                self->do_connect(endpoints);
            });
    }

    void ClientSession::finish(asio::error_code ec, std::unique_ptr<IsoMsg> response)
    {
        if (finished_)
        {
            return;
        }

        finished_ = true;

        asio::error_code ignored;

        timer_.cancel(ignored);
        socket_.cancel(ignored);
        socket_.close(ignored);

        if (config_->handler)
        {
            config_->handler(ec, iso_handler_, std::move(response));
        }
    }

    void ClientSession::do_connect(const tcp::resolver::results_type &endpoints)
    {
        auto self = shared_from_this();

        asio::async_connect(
            socket_,
            endpoints,
            [self](const asio::error_code &ec, const tcp::endpoint &)
            {
                if (ec)
                {
                    self->finish(ec, nullptr);
                    return;
                }

                self->do_write();
            });
    }

    void ClientSession::do_write()
    {

        auto self = shared_from_this();
        auto len = body_len_;
        uint8_t hdr[2];
        hdr[0] = static_cast<uint8_t>((len >> 8) & 0xFF);
        hdr[1] = static_cast<uint8_t>(len & 0xFF);

        std::array<asio::const_buffer, 2> buffers = {
            asio::buffer(hdr, 2),
            asio::buffer(body_, len)};
        asio::async_write(socket_, buffers,
                          [this, self, len](std::error_code ec, std::size_t)
                          {
                              if (ec)
                              {
                                  self->finish(ec, nullptr);
                                  return;
                              }

                              self->read_header();
                          });
    }

    void ClientSession::read_header()
    {
        auto self = shared_from_this();
        asio::async_read(socket_, asio::buffer(header_, 2),
                         [this, self](std::error_code ec, std::size_t)
                         {
                             if (ec)
                             {
                                 self->finish(ec, nullptr);
                                 return;
                             }

                             uint16_t len = (static_cast<uint16_t>(header_[0]) << 8) | header_[1];
                             if (len == 0 || len > MAX_LEN)
                             {
                                 return;
                             }

                             read_body(len);
                         });
    }

    void ClientSession::read_body(uint16_t len)
    {
        auto self = shared_from_this();

        asio::async_read(socket_, asio::buffer(asio::buffer(body_, len)),
                         [this, self, len](std::error_code ec, std::size_t length)
                         {
                             if (ec)
                             {
                                 self->finish(ec, nullptr);
                             }
                             else
                             {
                                 auto iso_msg = std::make_unique<IsoMsg>();
                                 (void)DL_ISO8583_MSG_Unpack(&iso_handler_, body_, body_len_, iso_msg->get());
                                 self->finish({}, std::move(iso_msg));
                             }
                         });
    }

    void ClientSession::set_timeout(std::chrono::milliseconds dur)
    {
        timeout_flg_ = false;
        timer_.expires_after(dur);
        timer_.async_wait([this, self = shared_from_this()](asio::error_code ec)
                          {
            
            std::lock_guard<std::mutex> lock(mtx_);
            if(timeout_flg_) return;
            timeout_flg_=true;
            if (!ec) 
            {
                auto iso_timeout_msg = std::make_unique<IsoMsg>();
                self->finish(
                    asio::error::make_error_code(asio::error::timed_out),
                    std::move(iso_timeout_msg)
                );

            } 
            else
            {
                self->finish(
                    ec,
                    nullptr
                );
            } });
    }

    // -------------------------
    // Session
    // -------------------------

    Session::Session(tcp::socket socket, asio::io_context &io_context, asio::thread_pool &worker_pool, std::shared_ptr<Config> config)
        : socket_(std::move(socket)), io_context_(io_context), worker_pool_(worker_pool), config_(config), timer_(socket_.get_executor())
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
                                 return;
                             uint16_t len = (static_cast<uint16_t>(header_[0]) << 8) | header_[1];
                             if (len == 0 || len > MAX_LEN)
                             {
                                 return;
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
                                 return;
                             body_len_ = length;
                             set_timeout(std::chrono::milliseconds(config_->timeout));
                             asio::post(self->worker_pool_, [self, this]()
                                        { self->handle_message(body_, body_len_); });
                         });
    }

    void Session::handle_message(const uint8_t *data, size_t len)
    {
        auto self = shared_from_this();
        DL_ISO8583_MSG iso_msg;
        DL_ISO8583_MSG_Init(NULL, 0, &iso_msg);
        (void)DL_ISO8583_MSG_Unpack(&iso_handler_, data, len, &iso_msg);

        config_->processor(iso_handler_, iso_msg);
        {
            timer_.cancel();
            std::lock_guard<std::mutex> lock(mtx_);
            if (!timeout_flg_)
            {
                timeout_flg_ = true;
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
        uint8_t hdr[2];
        hdr[0] = static_cast<uint8_t>((len >> 8) & 0xFF);
        hdr[1] = static_cast<uint8_t>(len & 0xFF);

        std::array<asio::const_buffer, 2> buffers = {
            asio::buffer(hdr, 2),
            asio::buffer(data, len)};
        asio::async_write(socket_, buffers,
                          [this, self, data, len](std::error_code ec, std::size_t)
                          {
                              if (ec)
                                  return;

                              if (config_->is_persist_connection)
                                  read_header();
                          });
    }

    void Session::set_timeout(std::chrono::milliseconds dur)
    {
        timeout_flg_ = false;
        timer_.expires_after(dur);
        timer_.async_wait([this, self = shared_from_this()](asio::error_code ec)
                          {
            if (!ec) 
            {
                std::lock_guard<std::mutex> lock(mtx_);
                if(timeout_flg_) return;
                timeout_flg_=true;
                DL_ISO8583_MSG iso_timeout_msg;       
                DL_ISO8583_MSG_Init(NULL, 0, &iso_timeout_msg);

                (void)DL_ISO8583_MSG_Unpack(&iso_handler_, body_, body_len_, &iso_timeout_msg);
                config_->timeout_processor(iso_handler_, iso_timeout_msg);

                DL_UINT8 packBuf[MAX_LEN];
                DL_UINT16 packedSize;

                (void)DL_ISO8583_MSG_Pack(&iso_handler_, &iso_timeout_msg, packBuf, &packedSize);

                asio::post(io_context_, [self = shared_from_this(), packBuf, packedSize]()
                        { self->send_response(packBuf, packedSize); });

                DL_ISO8583_MSG_Free(&iso_timeout_msg);
            } });
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
    // Client
    // -------------------------

    Client::Client(asio::io_context &io, std::shared_ptr<Config> config)
        : io_(io), worker_pool_(config->worker_count), config_(config), workGuard_(asio::make_work_guard(io_)),
          stopped_(false)
    {
    }

    Client::~Client()
    {
        stop();
    }

    void Client::stop()
    {
        bool expected = false;

        if (!stopped_.compare_exchange_strong(expected, true))
        {
            return;
        }

        workGuard_.reset();
        io_.stop();
    }

    void Client::send(std::unique_ptr<IsoMsg> request)
    {
        if (stopped_.load())
        {
            throw std::runtime_error("Iso Client is stopped!");
        }

        asio::post(io_, [this, request = std::move(request)]() mutable
                   { 
                    std::make_shared<ClientSession>(io_, worker_pool_, config_)->send_async(std::move(request));
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
    void Processor::start(std::shared_ptr<Config> config,bool wait_flg)
    {
        config_ = config;
        try
        {
            if (config_->is_server)
            {
                server_= std::make_unique<Server>(io_, config_);
            }
            else
            {
                client_= std::make_unique<Client>(io_, config_);
            }

            threads_.reserve(config_->thread_count);
            for (int i = 0; i < config_->thread_count; ++i)
            {
                threads_.emplace_back([&]()
                                        { io_.run(); });
            }
            if(wait_flg)
            {
                for (auto &t : threads_)
                    t.join();
            }
            else
            {
                for (auto &t : threads_)
                    t.detach();
            }           
        }
        catch (const std::exception &e)
        {
            std::cerr << "Fatal: " << e.what() << std::endl;
        }
    }

    void Processor::send(std::unique_ptr<IsoMsg> request)
    {
        if(client_) client_->send(std::move(request));
    }
}
