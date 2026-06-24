#pragma once
// Mooncake distributed tracing (OpenTelemetry).
//
// Design goals:
//   * Call sites never need #ifdef. When built WITHOUT -DWITH_TRACING the
//     scopes below are empty no-ops the compiler erases (zero overhead).
//   * No OpenTelemetry / yalantinglibs headers leak through this header (PIMPL),
//     so including it stays cheap across the codebase.
//
// Carrier: the W3C `traceparent` string, passed in the coro_rpc *request
// attachment* (Mooncake does not otherwise use the attachment).
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace mooncake::tracing {

// Initialise the global tracer once at process start. No-op unless built with
// WITH_TRACING and `otlp_endpoint` is non-empty.
//   service_name : e.g. "mooncake-master" / "mooncake-client"
//   otlp_endpoint: e.g. "http://127.0.0.1:4318/v1/traces" (empty => disabled)
void InitTracing(const std::string& service_name,
                 const std::string& otlp_endpoint);

bool Enabled();

// RAII span for an *incoming* RPC, created inside a (synchronous) coro_rpc
// handler. The ctor reads the request attachment (traceparent) from the current
// coro_rpc context and parents the span to the remote caller; the dtor ends it.
class ServerSpanScope {
 public:
  explicit ServerSpanScope(std::string_view rpc_name);
  ~ServerSpanScope();
  ServerSpanScope(const ServerSpanScope&) = delete;
  ServerSpanScope& operator=(const ServerSpanScope&) = delete;

  // Attach a child sub-step span (Phase 2). Returns an opaque RAII handle.
  class Step {
   public:
    explicit Step(std::string_view name);
    ~Step();
    Step(const Step&) = delete;
    Step& operator=(const Step&) = delete;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
  };

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// In-process operation span. Becomes a child of the current active span AND the
// new active span, so anything created inside it -- master RPC client spans
// (ClientSpanScope) and data-transfer spans -- nests under it. Use it to fold a
// high-level store op (Client::Put) plus the master RPCs and the actual data
// transfer it triggers into ONE trace/waterfall. Safe because store Client
// methods run synchronously on the calling thread.
class OpScope {
 public:
  explicit OpScope(std::string_view name);
  ~OpScope();
  OpScope(const OpScope&) = delete;
  OpScope& operator=(const OpScope&) = delete;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// RAII span for an *outgoing* RPC. After construction, traceparent() returns the
// string the caller must place in the coro_rpc request attachment.
class ClientSpanScope {
 public:
  explicit ClientSpanScope(std::string_view rpc_name);
  ~ClientSpanScope();
  ClientSpanScope(const ClientSpanScope&) = delete;
  ClientSpanScope& operator=(const ClientSpanScope&) = delete;

  const std::string& traceparent() const { return traceparent_; }

 private:
  std::string traceparent_;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::tracing
