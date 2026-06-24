// Tiny client to validate distributed tracing against a real mooncake_master.
// Exercises BOTH instrumented choke points:
//   * client: MasterClient::invoke_rpc -> ClientSpanScope + set_req_attachment
//   * server: execute_rpc -> ServerSpanScope (in the real master binary)
//
// Usage: mc_trace_test <master_addr> <otlp_endpoint>
//   e.g. mc_trace_test 127.0.0.1:50051 http://127.0.0.1:14318/v1/traces
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "master_client.h"
#include "tracing.h"
#include "types.h"

int main(int argc, char** argv) {
    std::string master_addr = (argc > 1) ? argv[1] : "127.0.0.1:50051";
    std::string endpoint = (argc > 2) ? argv[2] : "";

    mooncake::tracing::InitTracing("mooncake-client", endpoint);

    mooncake::MasterClient client(mooncake::generate_uuid());
    auto ec = client.Connect(master_addr);
    if (ec != mooncake::ErrorCode::OK) {
        std::cout << "[test-client] Connect FAILED to " << master_addr
                  << " (ErrorCode=" << static_cast<int>(ec) << ")\n";
        return 1;
    }
    std::cout << "[test-client] connected to " << master_addr << "\n";

    // (1) single RPC -> traced via invoke_rpc / execute_rpc
    auto single = client.ExistKey("trace-demo-single-key");
    std::cout << "[test-client] ExistKey -> "
              << (single.has_value() ? (single.value() ? "true" : "false")
                                     : "rpc-error")
              << "\n";

    // (2) batch RPC -> traced via invoke_batch_rpc + batch handler ServerSpanScope,
    //     with a nested "lookup_keys" sub-step span (3-level waterfall).
    std::vector<std::string> keys;
    for (int i = 0; i < 8; ++i) keys.push_back("trace-demo-batch-key-" + std::to_string(i));
    auto batch = client.BatchExistKey(keys);
    std::cout << "[test-client] BatchExistKey(" << keys.size() << " keys) -> "
              << batch.size() << " results\n";

    std::cout << "[test-client] done\n";
    return 0;
}
