// ASIO reference probe: strand dispatch/post inside a handler.

#include "asio.hpp"

#include <cstdio>
#include <mutex>
#include <vector>

int main()
{
    asio::io_context io;
    asio::strand<asio::io_context::executor_type> strand(io.get_executor());
    std::vector<int> order;
    std::mutex mutex;

    asio::post(strand, [&]() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            order.push_back(1);
        }
        // dispatch inside the strand runs inline before this handler returns.
        asio::dispatch(strand, [&]() {
            std::lock_guard<std::mutex> lock(mutex);
            order.push_back(2);
        });
        // post inside the strand is queued, not inline.
        asio::post(strand, [&]() {
            std::lock_guard<std::mutex> lock(mutex);
            order.push_back(3);
        });
    });

    io.run();

    std::printf("order:");
    for (int v : order)
        std::printf(" %d", v);
    std::printf("\n");

    return 0;
}
