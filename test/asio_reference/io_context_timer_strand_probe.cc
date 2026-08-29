// ASIO reference probe: io_context, steady_timer, strand.
// Prints observable behavior that AsyncCore must reproduce.

#include "asio.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

static asio::io_context io;
static asio::steady_timer timer(io);
static std::atomic<int> timer_ok{0};
static std::atomic<int> timer_cancel{0};

static void timer_expire_probe()
{
    timer.expires_after(std::chrono::milliseconds(10));
    timer.async_wait([](const asio::error_code &ec) {
        std::printf("timer_expire: ec=%d\n", ec.value());
        if (!ec)
            ++timer_ok;
    });
    io.run();
    std::printf("timer_expire: ok=%d\n", timer_ok.load());
}

static void timer_cancel_probe()
{
    timer.expires_after(std::chrono::hours(1));
    timer.async_wait([](const asio::error_code &ec) {
        std::printf("timer_cancel: ec=%d\n", ec.value());
        if (ec == asio::error::operation_aborted)
            ++timer_cancel;
    });
    std::size_t n = timer.cancel();
    io.restart();
    io.run();
    std::printf("timer_cancel: cancelled_count=%zu total=%d\n", n,
                timer_cancel.load());
}

static void timer_reuse_probe()
{
    timer.expires_after(std::chrono::milliseconds(5));
    timer.async_wait([](const asio::error_code &ec) {
        std::printf("timer_reuse_first: ec=%d\n", ec.value());
        // Re-arm from inside the handler.
        timer.expires_after(std::chrono::milliseconds(5));
        timer.async_wait([](const asio::error_code &ec2) {
            std::printf("timer_reuse_second: ec=%d\n", ec2.value());
            ++timer_ok;
        });
    });
    io.restart();
    io.run();
    std::printf("timer_reuse: ok=%d\n", timer_ok.load());
}

static void strand_order_probe()
{
    asio::io_context io2;
    asio::strand<asio::io_context::executor_type> strand(io2.get_executor());
    std::mutex print_mutex;
    std::vector<int> order;

    for (int i = 0; i < 10; ++i)
    {
        asio::post(strand, [&, i]() {
            std::lock_guard<std::mutex> lock(print_mutex);
            order.push_back(i);
        });
    }

    std::thread t1([&] { io2.run(); });
    std::thread t2([&] { io2.run(); });
    t1.join();
    t2.join();

    std::printf("strand_order:");
    for (int v : order)
        std::printf(" %d", v);
    std::printf("\n");
}

int main()
{
    std::printf("=== io_context/timer/strand ASIO reference ===\n");
    timer_expire_probe();
    timer_cancel_probe();
    timer_reuse_probe();
    strand_order_probe();
    return 0;
}
