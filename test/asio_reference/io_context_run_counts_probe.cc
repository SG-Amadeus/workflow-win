// ASIO reference probe: io_context run/poll/run_one return counts.

#include "asio.hpp"

#include <cstdio>
#include <atomic>

static std::atomic<int> count{0};

static void inc()
{
    ++count;
}

int main()
{
    asio::io_context io;

    // run returns number of handlers executed.
    asio::post(io, inc);
    asio::post(io, inc);
    asio::post(io, inc);
    std::size_t n = io.run();
    std::printf("run_count=%zu executed=%d\n", n, count.load());
    count = 0;

    // run_one returns 1.
    asio::post(io, inc);
    io.restart();
    std::size_t one = io.run_one();
    std::printf("run_one=%zu executed=%d\n", one, count.load());
    count = 0;
    io.restart();

    // poll processes ready handlers.
    asio::post(io, inc);
    asio::post(io, inc);
    std::size_t p = io.poll();
    std::printf("poll_count=%zu executed=%d\n", p, count.load());
    count = 0;
    io.restart();

    // poll_one processes one.
    asio::post(io, inc);
    std::size_t p1 = io.poll_one();
    std::printf("poll_one=%zu executed=%d\n", p1, count.load());

    return 0;
}
