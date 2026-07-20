#include <gflags/gflags.h>
#include <glog/logging.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "types.h"

USE_engine_flags;
DEFINE_string(master_server_entry, "etcd://127.0.0.1:2379",
              "Master server entry");
DEFINE_string(control_dir, "", "Directory containing workload controls");
DEFINE_string(key_prefix, "oplog-concurrent", "Workload key prefix");
DEFINE_int32(client_port, 19001, "TCP port in the client segment name");
DEFINE_int32(threads, 16, "Number of concurrent Put/Get workers");
DEFINE_uint64(segment_bytes, 512ULL * 1024 * 1024,
              "Mounted memory segment size");
DEFINE_uint64(value_bytes, 64, "Value size for each Put");
DEFINE_uint64(ready_gets_per_thread, 100,
              "Successful pre-failover Gets required per worker");
DEFINE_int32(max_runtime_sec, 180, "Workload safety timeout");

namespace mooncake::testing {
namespace {

namespace fs = std::filesystem;

constexpr size_t kPhaseCount = 3;
const std::array<std::string, kPhaseCount> kPhaseNames{"pre", "fault", "post"};

struct PhaseStats {
    std::atomic<uint64_t> attempts{0};
    std::atomic<uint64_t> put_ok{0};
    std::atomic<uint64_t> get_ok{0};
    std::atomic<uint64_t> get_errors{0};
    std::atomic<uint64_t> mismatch{0};
};

void WriteFile(const fs::path& path, const std::string& contents) {
    std::ofstream output(path);
    output << contents << '\n';
}

int ReadPhase(const fs::path& phase_path, int fallback) {
    std::ifstream input(phase_path);
    std::string phase;
    input >> phase;
    for (size_t i = 0; i < kPhaseNames.size(); ++i) {
        if (phase == kPhaseNames[i]) return static_cast<int>(i);
    }
    return fallback;
}

std::string MakeValue(const std::string& key, size_t size) {
    std::string value;
    value.reserve(size);
    while (value.size() < size) {
        value.append(key);
        value.push_back('|');
    }
    value.resize(size);
    return value;
}

class ConcurrentFailoverWorkload {
   public:
    ConcurrentFailoverWorkload(std::shared_ptr<ClientTestWrapper> client,
                               fs::path control_dir, std::string key_prefix,
                               size_t thread_count, size_t value_size,
                               uint64_t ready_gets_per_thread)
        : client_(std::move(client)),
          control_dir_(std::move(control_dir)),
          key_prefix_(std::move(key_prefix)),
          thread_count_(thread_count),
          value_size_(value_size),
          ready_gets_per_thread_(ready_gets_per_thread) {}

    bool Run(std::chrono::seconds max_runtime) {
        WriteFile(control_dir_ / "started", "started");
        StartWorkers();

        const auto deadline = std::chrono::steady_clock::now() + max_runtime;
        while (!fs::exists(control_dir_ / "stop") &&
               std::chrono::steady_clock::now() < deadline) {
            const int current = phase_.load(std::memory_order_relaxed);
            phase_.store(ReadPhase(control_dir_ / "phase", current),
                         std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        timed_out_ = !fs::exists(control_dir_ / "stop");
        stop_.store(true, std::memory_order_relaxed);
        for (auto& worker : workers_) worker.join();

        RunFinalProbe();
        const std::string summary = BuildSummary();
        WriteFile(control_dir_ / "summary.json", summary);
        std::cout << summary << std::endl;
        return !timed_out_;
    }

   private:
    void StartWorkers() {
        workers_.reserve(thread_count_);
        for (size_t lane = 0; lane < thread_count_; ++lane) {
            workers_.emplace_back([this, lane] { Worker(lane); });
        }
    }

    void Worker(size_t lane) {
        uint64_t sequence = 0;
        uint64_t pre_get_ok = 0;
        bool lane_ready = false;
        while (!stop_.load(std::memory_order_relaxed)) {
            const int phase_index = phase_.load(std::memory_order_relaxed);
            ++sequence;
            std::ostringstream key_stream;
            key_stream << key_prefix_ << '-';
            if (lane < 10) key_stream << '0';
            key_stream << lane << '-' << sequence;
            const std::string key = key_stream.str();
            const std::string value = MakeValue(key, value_size_);

            auto& current = stats_[phase_index];
            current.attempts.fetch_add(1, std::memory_order_relaxed);
            const ErrorCode put_error = client_->Put(key, value);
            if (put_error != ErrorCode::OK) {
                RecordError(phase_index, "put", put_error);
                continue;
            }
            current.put_ok.fetch_add(1, std::memory_order_relaxed);
            MarkReconnectedIfReady();

            std::string actual;
            const ErrorCode get_error = client_->Get(key, actual);
            if (get_error != ErrorCode::OK) {
                current.get_errors.fetch_add(1, std::memory_order_relaxed);
                RecordError(phase_index, "get", get_error);
                continue;
            }
            if (actual != value) {
                current.mismatch.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            current.get_ok.fetch_add(1, std::memory_order_relaxed);
            if (phase_index == 0) {
                ++pre_get_ok;
                if (!lane_ready && pre_get_ok >= ready_gets_per_thread_) {
                    lane_ready = true;
                    const size_t count = ready_lanes_.fetch_add(1) + 1;
                    if (count == thread_count_) {
                        WriteFile(control_dir_ / "ready", "ready");
                    }
                }
            }
        }
    }

    void RecordError(int phase_index, const char* operation, ErrorCode error) {
        std::lock_guard<std::mutex> lock(error_mutex_);
        ++error_counts_[kPhaseNames[phase_index] + ":" + operation + ":" +
                        toString(error)];
    }

    void MarkReconnectedIfReady() {
        if (!fs::exists(control_dir_ / "leader_ready")) return;
        bool expected = false;
        if (reconnected_.compare_exchange_strong(expected, true)) {
            WriteFile(control_dir_ / "reconnected", "reconnected");
        }
    }

    void RunFinalProbe() {
        const std::string probe_key = key_prefix_ + "-final-probe";
        const std::string probe_value = "final-value";
        probe_put_ = client_->Put(probe_key, probe_value);
        std::string actual;
        probe_get_ = client_->Get(probe_key, actual);
        if (probe_get_ == ErrorCode::OK && actual != probe_value) {
            probe_get_ = ErrorCode::INTERNAL_ERROR;
        }
        probe_batch_ = client_->BatchSmoke(key_prefix_ + "-final-batch");
    }

    std::string BuildSummary() const {
        std::ostringstream output;
        output << "{\"threads\":" << thread_count_
               << ",\"timed_out\":" << (timed_out_ ? "true" : "false")
               << ",\"reconnected\":"
               << (reconnected_.load() ? "true" : "false") << ",\"phases\":{";
        for (size_t i = 0; i < stats_.size(); ++i) {
            if (i != 0) output << ',';
            output << '\"' << kPhaseNames[i] << "\":{";
            output << "\"attempts\":" << stats_[i].attempts.load() << ',';
            output << "\"put_ok\":" << stats_[i].put_ok.load() << ',';
            output << "\"get_ok\":" << stats_[i].get_ok.load() << ',';
            output << "\"get_errors\":" << stats_[i].get_errors.load() << ',';
            output << "\"mismatch\":" << stats_[i].mismatch.load() << '}';
        }
        output << "},\"errors\":{";
        bool first = true;
        for (const auto& [error, count] : error_counts_) {
            if (!first) output << ',';
            first = false;
            output << '\"' << error << "\":" << count;
        }
        output << "},\"probe\":{";
        output << "\"put\":\"" << toString(probe_put_) << "\",";
        output << "\"get\":\"" << toString(probe_get_) << "\",";
        output << "\"batch_smoke\":\"" << toString(probe_batch_) << "\"}}";
        return output.str();
    }

    std::shared_ptr<ClientTestWrapper> client_;
    fs::path control_dir_;
    std::string key_prefix_;
    size_t thread_count_;
    size_t value_size_;
    uint64_t ready_gets_per_thread_;
    std::array<PhaseStats, kPhaseCount> stats_;
    std::atomic<int> phase_{0};
    std::atomic<bool> stop_{false};
    std::atomic<bool> reconnected_{false};
    std::atomic<size_t> ready_lanes_{0};
    std::vector<std::thread> workers_;
    mutable std::mutex error_mutex_;
    std::map<std::string, uint64_t> error_counts_;
    bool timed_out_{false};
    ErrorCode probe_put_{ErrorCode::INTERNAL_ERROR};
    ErrorCode probe_get_{ErrorCode::INTERNAL_ERROR};
    ErrorCode probe_batch_{ErrorCode::INTERNAL_ERROR};
};

}  // namespace
}  // namespace mooncake::testing

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    if (FLAGS_control_dir.empty() || FLAGS_threads <= 0 ||
        FLAGS_segment_bytes == 0 || FLAGS_value_bytes == 0 ||
        FLAGS_ready_gets_per_thread == 0 || FLAGS_max_runtime_sec <= 0) {
        LOG(ERROR) << "Invalid workload flags";
        return 2;
    }

    namespace fs = std::filesystem;
    const fs::path control_dir(FLAGS_control_dir);
    fs::create_directories(control_dir);
    mooncake::testing::WriteFile(control_dir / "phase", "pre");

    const std::string hostname =
        "localhost:" + std::to_string(FLAGS_client_port);
    auto client = mooncake::testing::ClientTestWrapper::CreateClientWrapper(
        hostname, FLAGS_engine_meta_url, FLAGS_protocol, FLAGS_device_name,
        FLAGS_master_server_entry);
    if (!client.has_value()) {
        LOG(ERROR) << "Failed to create workload client";
        return 2;
    }

    void* segment = nullptr;
    const mooncake::ErrorCode mount_error =
        (*client)->Mount(FLAGS_segment_bytes, segment);
    if (mount_error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "Failed to mount workload segment: "
                   << mooncake::toString(mount_error);
        return 2;
    }

    mooncake::testing::ConcurrentFailoverWorkload workload(
        *client, control_dir, FLAGS_key_prefix,
        static_cast<size_t>(FLAGS_threads),
        static_cast<size_t>(FLAGS_value_bytes), FLAGS_ready_gets_per_thread);
    const bool completed =
        workload.Run(std::chrono::seconds(FLAGS_max_runtime_sec));
    return completed ? 0 : 2;
}
