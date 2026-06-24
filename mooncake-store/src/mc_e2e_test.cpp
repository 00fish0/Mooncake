// End-to-end Mooncake store test: real master + real store Client over TCP,
// covering put / get / exist on the actual data path (Transfer Engine).
//
// Also exercises distributed tracing: with --otlp_endpoint set on both this
// driver and the master, each put/get/exist produces a linked client->master
// trace (PutStart/PutEnd, GetReplicaList, ExistKey ...) in Jaeger.
//
// Usage: mc_e2e_test <master_addr> <metadata_url> <otlp_endpoint>
//   e.g. mc_e2e_test 127.0.0.1:51100 http://127.0.0.1:18080/metadata \
//                    http://127.0.0.1:14318/v1/traces
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "tracing.h"
#include "types.h"
#include "utils.h"

using namespace mooncake;

int main(int argc, char** argv) {
    std::string master_addr = (argc > 1) ? argv[1] : "127.0.0.1:51100";
    std::string metadata = (argc > 2) ? argv[2] : "http://127.0.0.1:18080/metadata";
    std::string endpoint = (argc > 3) ? argv[3] : "";
    const std::string protocol = "tcp";
    const std::string host = "127.0.0.1";

    mooncake::tracing::InitTracing("mooncake-client", endpoint);

    auto client_opt =
        Client::Create(host, metadata, protocol, std::nullopt, master_addr);
    if (!client_opt) {
        std::cerr << "[e2e] Client::Create FAILED\n";
        return 1;
    }
    auto client = *client_opt;
    std::cout << "[e2e] client created (master=" << master_addr
              << ", metadata=" << metadata << ")\n";

    // Contribute a 512MB segment so puts have somewhere to land.
    const size_t seg_size = 512ull * 1024 * 1024;
    void* seg = allocate_buffer_allocator_memory(seg_size);
    if (!seg) {
        std::cerr << "[e2e] segment alloc FAILED\n";
        return 1;
    }
    auto mr = client->MountSegment(seg, seg_size, protocol);
    if (!mr.has_value()) {
        std::cerr << "[e2e] MountSegment FAILED: " << toString(mr.error()) << "\n";
        return 1;
    }
    std::cout << "[e2e] segment mounted (512MB)\n";

    // Register a local buffer region used for put/get slices.
    const size_t buf_size = 128ull * 1024 * 1024;
    auto alloc = std::make_unique<SimpleAllocator>(buf_size);
    auto rr = client->RegisterLocalMemory(alloc->getBase(), buf_size, "cpu:0",
                                          false, false);
    if (!rr.has_value()) {
        std::cerr << "[e2e] RegisterLocalMemory FAILED: " << toString(rr.error())
                  << "\n";
        return 1;
    }
    std::cout << "[e2e] local memory registered (128MB)\n";

    const std::string key = "e2e-demo-key";
    const std::string data = "hello-mooncake-end-to-end-put-get-exist-payload";
    bool ok = true;

    // ---- PUT ----
    void* pbuf = alloc->allocate(data.size());
    memcpy(pbuf, data.data(), data.size());
    std::vector<Slice> pslices{Slice{pbuf, data.size()}};
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto put_r = client->Put(key, pslices, cfg);
    std::cout << "[e2e] PUT   " << key << " (" << data.size() << "B) -> "
              << (put_r.has_value() ? "OK" : toString(put_r.error())) << "\n";
    alloc->deallocate(pbuf, data.size());
    ok &= put_r.has_value();

    // ---- EXIST (expect true) ----
    auto ex1 = client->IsExist(key);
    std::cout << "[e2e] EXIST " << key << " -> "
              << (ex1.has_value() ? (ex1.value() ? "true" : "false") : "err")
              << " (expect true)\n";
    ok &= ex1.has_value() && ex1.value();

    // ---- GET + verify ----
    void* gbuf = alloc->allocate(data.size());
    std::vector<Slice> gslices{Slice{gbuf, data.size()}};
    auto get_r = client->Get(key, gslices);
    bool match = get_r.has_value() && !gslices.empty() &&
                 gslices[0].size == data.size() &&
                 memcmp(gbuf, data.data(), data.size()) == 0;
    std::cout << "[e2e] GET   " << key << " -> "
              << (get_r.has_value() ? "OK" : toString(get_r.error()))
              << ", data " << (match ? "MATCH" : "MISMATCH") << "\n";
    alloc->deallocate(gbuf, data.size());
    ok &= match;

    // ---- EXIST nonexistent (expect false) ----
    auto ex2 = client->IsExist("no-such-key");
    std::cout << "[e2e] EXIST no-such-key -> "
              << (ex2.has_value() ? (ex2.value() ? "true" : "false") : "err")
              << " (expect false)\n";

    std::cout << "[e2e] " << (ok ? "ALL PASSED" : "FAILURES") << "\n";
    return ok ? 0 : 2;
}
