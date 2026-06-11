
#include "processor.h"
#include <format>
#include <charconv>
#include <cstdint>

using asio::ip::tcp;

namespace YSH
{
    // -------------------------
    // ClientSession
    // -------------------------

    ClientSession::ClientSession(asio::io_context &io_context, std::shared_ptr<Config> config)
        : socket_(io_context), io_context_(io_context), config_(config), strand_(asio::make_strand(socket_.get_executor()))
    {
        iso_handler_ = std::make_shared<DL_ISO8583_HANDLER>();
        DL_ISO8583_DEFS_1993_GetHandler(iso_handler_.get());        
    }

    ClientSession::~ClientSession()
    {
    }

    void ClientSession::start()
    {
        std::cout << "START..." << std::endl;
        tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(config_->host, std::to_string(config_->port));
        do_connect(endpoints);
    }

    void ClientSession::send(std::unique_ptr<IsoMsg> request)
    {
        auto self = shared_from_this();
       

        auto state = std::make_shared<MessageTaskState>();

        state->timer = std::make_shared<asio::steady_timer>(io_context_, std::chrono::milliseconds(config_->timeout));
        
        auto packBuf=std::make_unique<DL_UINT8[]>(MAX_LEN);
	    DL_UINT16          packedSize;
        (void)DL_ISO8583_MSG_Pack(request->getHandler(),request->get(),packBuf.get(),&packedSize);        

        state->timer->async_wait([state, self, packBuf=std::move(packBuf),packedSize](const std::error_code &ec) mutable
                                 {
        auto iso_timeout_msg=std::make_unique<IsoMsg>(self->iso_handler_);       

        (void)DL_ISO8583_MSG_Unpack(iso_timeout_msg->getHandler(), packBuf.get(), packedSize, iso_timeout_msg->get());
        if (!ec && !state->is_completed) {
            state->is_timed_out = true;
            
            if(self->config_->timeout_processor) iso_timeout_msg = self->config_->timeout_processor(std::move(iso_timeout_msg));

            asio::post(self->strand_, [self , iso_timeout_msg = std::move(iso_timeout_msg)]() mutable
            { 
                self->finish({}, std::move(iso_timeout_msg)); 
            });
        } });
        auto stan =getStan(request->get());
        //std::cout << "Emplace Stan:" << stan << std::endl;
        message_state_->emplace(stan, state);

        asio::post(self->strand_, [self , request = std::move(request)]() mutable
            { 
                self->do_write(std::move(request));
            }); 

           // do_write(std::move(request));
        
    }

    void ClientSession::finish(asio::error_code ec, std::unique_ptr<IsoMsg> response)
    {
     /*   asio::error_code ignored;

        socket_.cancel(ignored);
        socket_.close(ignored);
*/
        if (config_->handler)
        {
            config_->handler(ec, std::move(response));
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
                self->read_header();
            });
    }

    void ClientSession::do_write(std::unique_ptr<IsoMsg> iso_msg)
    {
         DL_UINT16 len;
        uint8_t body[MAX_LEN];
        (void)DL_ISO8583_MSG_Pack(iso_msg->getHandler(), iso_msg->get(), body, &len);

        auto self = shared_from_this();
        uint8_t hdr[2];
        hdr[0] = static_cast<uint8_t>((len >> 8) & 0xFF);
        hdr[1] = static_cast<uint8_t>(len & 0xFF);

        std::array<asio::const_buffer, 2> buffers = {
            asio::buffer(hdr, 2),
            asio::buffer(body, len)};
       /* asio::async_write(socket_, buffers,
                          [this, self, len](std::error_code ec, std::size_t)
                          {
                              if (ec)
                              {
                                  self->finish(ec, nullptr);
                                  return;
                              }
                          });*/
            asio::async_write(socket_, buffers,
                asio::bind_executor(strand_,
                    [](std::error_code ec, std::size_t n) {
                        
                    }));
        
    }

    void ClientSession::read_header()
    {
        auto self = shared_from_this();
        auto header = std::make_shared<uint8_t[]>(2);

        asio::async_read(socket_, asio::buffer(header.get(), 2),
                         [this, header,self](std::error_code ec, std::size_t) 
                         {
                            if (ec == asio::error::connection_reset)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                                self->start();
                                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                            }
                             if (ec)
                             {
                                
                                 self->finish(ec, nullptr);
                                 return;
                             }

                             uint16_t len = (static_cast<uint16_t>(header.get()[0]) << 8) | header.get()[1];
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
        auto body = std::make_shared<uint8_t[]>(len);

        asio::async_read(socket_, asio::buffer(asio::buffer(body.get(), len)),
                         [this, self, body, len](std::error_code ec, std::size_t length)
                         {
                             if (ec)
                             {
                                 self->finish(ec, nullptr);
                             }
                             else
                             {
                                 auto iso_msg = std::make_unique<IsoMsg>(iso_handler_);
                                 (void)DL_ISO8583_MSG_Unpack(iso_msg->getHandler(), body.get(), length, iso_msg->get());
                                 auto stan = getStan(iso_msg->get());
                                // std::cout << "get STAN:" << stan << std::endl;
                                 auto it = message_state_->find(stan);
                                 if (it == message_state_->end())
                                 {
                                     std::cout << "Stan not found:" << stan << std::endl;
                                     self->read_header();
                                     return;
                                 }
                                
                                 if (it->second->is_timed_out)
                                 {
                                    if (it->second->timer)
                                    {
                                        std::error_code ignored;
                                        it->second->timer->cancel(ignored);
                                    }
                                    it->second.reset();
                                    message_state_->erase(it);
                                    self->read_header();
                                    return;
                                 }
                                 it->second->timer->cancel();
                                 it->second->is_completed = true;
                                 if (it->second->timer)
                                 {
                                     std::error_code ignored;
                                     it->second->timer->cancel(ignored);
                                 }
                                 it->second.reset();
                                 message_state_->erase(it);
                                 self->finish({}, std::move(iso_msg));
                             }
                             self->read_header();
                         });
    }

    // -------------------------
    // Session
    // -------------------------

    Session::Session(tcp::socket socket, asio::io_context &io_context, asio::thread_pool &worker_pool, std::shared_ptr<Config> config)
        : socket_(std::move(socket)), io_context_(io_context), worker_pool_(worker_pool), config_(config),
          strand_(asio::make_strand(socket_.get_executor()))
    {
        iso_handler_ = std::make_shared<DL_ISO8583_HANDLER>();
        DL_ISO8583_DEFS_1993_GetHandler(iso_handler_.get());
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
        auto body = std::make_shared<uint8_t[]>(len);


        asio::async_read(socket_, asio::buffer(asio::buffer(body.get(), len)),
                         [this, self, body](std::error_code ec, std::size_t length)
                         {
                             if (ec)
                                 return;
                             
                             // set_timeout(std::chrono::milliseconds(config_->timeout),body,len);
                             auto state = std::make_shared<MessageTaskState>();

                             state->timer = std::make_shared<asio::steady_timer>(io_context_, std::chrono::milliseconds(config_->timeout));

                             state->timer->async_wait([state, self, body, length](const std::error_code &ec)
                                                      {
                                if (!ec && !state->is_completed) {
                                    state->is_timed_out = true;
                                    auto iso_timeout_msg=std::make_unique<IsoMsg>(self->iso_handler_);       

                                    (void)DL_ISO8583_MSG_Unpack(iso_timeout_msg->getHandler(), body.get(), length, iso_timeout_msg->get());
                                    if(self->config_->timeout_processor) iso_timeout_msg = self->config_->timeout_processor(std::move(iso_timeout_msg));


                                    asio::post(self->strand_, [self ,iso_timeout_msg=std::move(iso_timeout_msg)]() mutable
                                            { 
                                                self->send_response( std::move(iso_timeout_msg)); 
                                            });
                                } });

                             auto iso_msg = std::make_unique<IsoMsg>(iso_handler_);
                            (void)DL_ISO8583_MSG_Unpack(iso_msg->getHandler(), body.get(), length, iso_msg->get());
                             asio::post(self->worker_pool_, [self, state, iso_msg=std::move(iso_msg), this]() mutable
                                        { 
                                            self->handle_message(state, std::move(iso_msg)); 
                                        });

                             if (config_->is_persist_connection)
                                 read_header();
                         });
    }

    void Session::handle_message(std::shared_ptr<MessageTaskState> state, std::unique_ptr<IsoMsg> iso_msg)
    {
        auto self = shared_from_this();
        

        if (config_->processor)
            iso_msg = config_->processor(std::move(iso_msg));

        if (state->is_timed_out)
        {
            return;
        }
        state->timer->cancel();
        state->is_completed = true;

       

        asio::post(strand_, [self = shared_from_this(), iso_msg=std::move(iso_msg)]() mutable
                   { self->send_response(std::move(iso_msg)); });
    }

    void Session::send_response(std::unique_ptr<IsoMsg> iso_msg)
    {
        auto self = shared_from_this();
        DL_UINT8 packBuf[MAX_LEN];
        DL_UINT16 packedSize;

        (void)DL_ISO8583_MSG_Pack(iso_msg->getHandler(), iso_msg->get(), packBuf, &packedSize);

        uint8_t hdr[2];
        hdr[0] = static_cast<uint8_t>((packedSize >> 8) & 0xFF);
        hdr[1] = static_cast<uint8_t>(packedSize & 0xFF);

        std::array<asio::const_buffer, 2> buffers = {
            asio::buffer(hdr, 2),
            asio::buffer(packBuf, packedSize)};
        asio::async_write(socket_, buffers,
                          [this, self](std::error_code ec, std::size_t)
                          {
                              if (ec)
                                  return;
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

    Server::~Server()
    {
        io_.stop();
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
        : io_(io), config_(config), workGuard_(asio::make_work_guard(io_)),
          stopped_(false)
    {
        client_session_ = std::make_shared<ClientSession>(io_, config_);
        client_session_->start();
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
                    client_session_->send(std::move(request));
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
    void Processor::start(std::shared_ptr<Config> config, bool wait_flg)
    {
        config_ = config;
        try
        {
            if (config_->is_server)
            {
                server_ = std::make_unique<Server>(io_, config_);
            }
            else
            {
                client_ = std::make_unique<Client>(io_, config_);
            }

            threads_.reserve(config_->thread_count);
            for (int i = 0; i < config_->thread_count; ++i)
            {
                threads_.emplace_back([&]()
                                      { io_.run(); });
            }
            if (wait_flg)
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
        if (client_)
            client_->send(std::move(request));
    }
}
