// Concurrent stress driver for the traced master: many put/get/exist ops across
// threads, so we can see what distributed traces look like under load in Jaeger.
//
// Usage: mc_stress <master> <metadata> <otlp_endpoint> <iters> <conc> <obj_bytes>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "tracing.h"
#include "types.h"
#include "utils.h"

using namespace mooncake;

int main(int argc, char** argv) {
    std::string master = (argc > 1) ? argv[1] : "127.0.0.1:51100";
    std::string metadata = (argc > 2) ? argv[2] : "http://127.0.0.1:18080/metadata";
    std::string endpoint = (argc > 3) ? argv[3] : "";
    int iters = (argc > 4) ? std::atoi(argv[4]) : 100;
    int conc = (argc > 5) ? std::atoi(argv[5]) : 4;
    size_t obj = (argc > 6) ? std::atoll(argv[6]) : 65536;
    const std::string proto = "tcp", host = "127.0.0.1";

    mooncake::tracing::InitTracing("mooncake-client", endpoint);
    auto co = Client::Create(host, metadata, proto, std::nullopt, master);
    if (!co) {
        std::cerr << "[stress] Client::Create FAILED\n";
        return 1;
    }
    auto client = *co;

    const size_t seg = 4ull << 30;  // 4GB storage segment
    void* sp = allocate_buffer_allocator_memory(seg);
    if (!client->MountSegment(sp, seg, proto).has_value()) {
        std::cerr << "[stress] MountSegment FAILED\n";
        return 1;
    }

    // Per-thread registered buffer to avoid slice-buffer contention.
    std::vector<std::unique_ptr<SimpleAllocator>> allocs;
    for (int t = 0; t < conc; ++t) {
        auto a = std::make_unique<SimpleAllocator>(64ull << 20);
        client->RegisterLocalMemory(a->getBase(), 64ull << 20, "cpu:0", false, false);
        allocs.push_back(std::move(a));
    }

    std::string payload(obj, 'x');
    std::atomic<long> ops{0}, errs{0};

    auto worker = [&](int tid) {
        auto* a = allocs[tid].get();
        for (int i = 0; i < iters; ++i) {
            std::string key = "stress-" + std::to_string(tid) + "-" + std::to_string(i);
            // PUT
            void* pb = a->allocate(obj);
            memcpy(pb, payload.data(), obj);
            std::vector<Slice> ps{Slice{pb, obj}};
            ReplicateConfig cfg;
            cfg.replica_num = 1;
            auto pr = client->Put(key, ps, cfg);
            a->deallocate(pb, obj);
            // EXIST
            auto ex = client->IsExist(key);
            // GET
            void* gb = a->allocate(obj);
            std::vector<Slice> gs{Slice{gb, obj}};
            auto gr = client->Get(key, gs);
            a->deallocate(gb, obj);
            ops.fetch_add(3);
            if (!pr.has_value() || !gr.has_value() || !ex.has_value()) errs.fetch_add(1);
        }
    };

    std::cout << "[stress] start: iters=" << iters << " conc=" << conc
              << " obj=" << obj << "B traced=" << (endpoint.empty() ? "no" : "yes")
              << std::endl;
    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> th;
    for (int t = 0; t < conc; ++t) th.emplace_back(worker, t);
    for (auto& t : th) t.join();
    double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    long total = ops.load();
    std::cout << "[stress] " << total << " ops (" << (iters * conc) << " put/get/exist each) in "
              << el << "s = " << (long)(total / el) << " ops/s, errors=" << errs.load()
              << std::endl;
    return 0;
}
