// ASIO reference probe: TCP connect/accept/read/write/cancel.

#include "asio.hpp"

#include <cstdio>
#include <thread>

using asio::ip::tcp;

static void print_ec(const char *tag, const asio::error_code &ec)
{
    std::printf("%s: value=%d message=\"%s\"\n", tag, ec.value(),
                ec.message().c_str());
}

int main()
{
    asio::io_context io;
    tcp::acceptor acceptor(io);
    tcp::endpoint endpoint(asio::ip::make_address("127.0.0.1"), 0);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(tcp::acceptor::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen();
    unsigned short port = acceptor.local_endpoint().port();

    // Accept cancel probe.
    bool accept_done = false;
    asio::error_code accept_ec;
    acceptor.async_accept([&](const asio::error_code &ec, tcp::socket) {
        accept_ec = ec;
        accept_done = true;
    });
    acceptor.cancel();
    io.run();
    print_ec("accept_cancel", accept_ec);
    std::printf("accept_cancel: done=%d\n", (int)accept_done);

    io.restart();

    // Normal accept + echo.
    tcp::socket server(io);
    bool accepted = false;
    acceptor.async_accept(server, [&](const asio::error_code &ec) {
        print_ec("accept", ec);
        accepted = true;
    });

    tcp::socket client(io);
    bool connected = false;
    client.async_connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"),
                                       port),
                         [&](const asio::error_code &ec) {
        print_ec("connect", ec);
        connected = true;
    });

    // Drive until accepted and connected.
    while (!accepted || !connected)
        io.run_one();

    char buf[16] = {};
    server.async_read_some(asio::buffer(buf), [&](const asio::error_code &ec,
                                                  std::size_t n) {
        print_ec("server_read", ec);
        std::printf("server_read: n=%zu buf=\"%.*s\"\n", n, (int)n, buf);
        asio::write(server, asio::buffer("world", 5));
    });

    client.async_write_some(asio::buffer("hello", 5),
                            [&](const asio::error_code &ec, std::size_t n) {
        print_ec("client_write", ec);
        std::printf("client_write: n=%zu\n", n);
    });

    char rbuf[16] = {};
    client.async_read_some(asio::buffer(rbuf),
                           [&](const asio::error_code &ec, std::size_t n) {
        print_ec("client_read", ec);
        std::printf("client_read: n=%zu buf=\"%.*s\"\n", n, (int)n, rbuf);
    });

    io.run();
    io.restart();

    // Cancel pending read.
    bool read_cancel_done = false;
    asio::error_code read_cancel_ec;
    client.async_read_some(asio::buffer(rbuf),
                           [&](const asio::error_code &ec, std::size_t) {
        read_cancel_ec = ec;
        read_cancel_done = true;
    });
    client.cancel();
    io.run();
    print_ec("client_read_cancel", read_cancel_ec);
    std::printf("client_read_cancel: done=%d\n", (int)read_cancel_done);

    client.close();
    server.close();
    acceptor.close();
    return 0;
}
