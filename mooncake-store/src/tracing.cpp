#include "tracing.h"

#ifndef WITH_TRACING
// ----------------------------------------------------------------------------
// No-op build: every scope is empty; the compiler erases all call sites.
// ----------------------------------------------------------------------------
namespace mooncake::tracing {
void InitTracing(const std::string&, const std::string&) {}
bool Enabled() { return false; }
struct ServerSpanScope::Impl {};
struct ServerSpanScope::Step::Impl {};
ServerSpanScope::ServerSpanScope(std::string_view) {}
ServerSpanScope::~ServerSpanScope() = default;
ServerSpanScope::Step::Step(std::string_view) {}
ServerSpanScope::Step::~Step() = default;
struct ClientSpanScope::Impl {};
ClientSpanScope::ClientSpanScope(std::string_view) {}
ClientSpanScope::~ClientSpanScope() = default;
struct OpScope::Impl {};
OpScope::OpScope(std::string_view) {}
OpScope::~OpScope() = default;
}  // namespace mooncake::tracing

#else
// ----------------------------------------------------------------------------
// Real build (-DWITH_TRACING): OpenTelemetry spans + a hand-rolled OTLP/HTTP
// JSON exporter (libcurl) so we don't depend on otel-cpp's protobuf exporter.
// ----------------------------------------------------------------------------
#include <atomic>
#include <chrono>
#include <cstdio>

#include <curl/curl.h>

#include <ylt/coro_rpc/coro_rpc_server.hpp>  // coro_rpc::detail::set_context

#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/scope.h"
#include "opentelemetry/trace/span_context.h"

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace otel_resource = opentelemetry::sdk::resource;
namespace otel_nostd = opentelemetry::nostd;
namespace otel_common = opentelemetry::sdk::common;

namespace mooncake::tracing {
namespace {

std::atomic<bool> g_enabled{false};

// otel-cpp (non-STL build) wants nostd::string_view, not std::string_view.
otel_nostd::string_view NS(std::string_view s) {
  return otel_nostd::string_view(s.data(), s.size());
}

otel_nostd::shared_ptr<trace_api::Tracer> Tracer() {
  return trace_api::Provider::GetTracerProvider()->GetTracer("mooncake", "0.1");
}

// ----- W3C traceparent: "00-<32hex trace>-<16hex span>-<2hex flags>" --------
std::string Inject(const trace_api::SpanContext& ctx) {
  char tid[32], sid[16];
  ctx.trace_id().ToLowerBase16(otel_nostd::span<char, 32>{tid});
  ctx.span_id().ToLowerBase16(otel_nostd::span<char, 16>{sid});
  std::string out = "00-";
  out.append(tid, 32);
  out.push_back('-');
  out.append(sid, 16);
  out.push_back('-');
  out += ctx.trace_flags().IsSampled() ? "01" : "00";
  return out;
}

uint8_t Nib(char c) {
  return (c >= '0' && c <= '9') ? uint8_t(c - '0') : uint8_t((c | 0x20) - 'a' + 10);
}

trace_api::SpanContext Extract(std::string_view tp) {
  if (tp.size() < 55) return trace_api::SpanContext::GetInvalid();
  uint8_t tb[16], sb[8];
  const char* t = tp.data() + 3;
  const char* s = tp.data() + 3 + 32 + 1;
  for (int i = 0; i < 16; ++i) tb[i] = (Nib(t[2 * i]) << 4) | Nib(t[2 * i + 1]);
  for (int i = 0; i < 8; ++i) sb[i] = (Nib(s[2 * i]) << 4) | Nib(s[2 * i + 1]);
  return trace_api::SpanContext(
      trace_api::TraceId(otel_nostd::span<const uint8_t, 16>{tb}),
      trace_api::SpanId(otel_nostd::span<const uint8_t, 8>{sb}),
      trace_api::TraceFlags(tp.back() == '1' ? 1 : 0), /*is_remote=*/true);
}

// ----- minimal OTLP/HTTP JSON exporter (libcurl) ----------------------------
std::string JEsc(const std::string& s) {
  std::string o;
  o.reserve(s.size() + 8);
  for (char c : s) {
    switch (c) {
      case '"': o += "\\\""; break;
      case '\\': o += "\\\\"; break;
      case '\n': o += "\\n"; break;
      case '\r': o += "\\r"; break;
      case '\t': o += "\\t"; break;
      default:
        if ((unsigned char)c < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o += b; }
        else o += c;
    }
  }
  return o;
}
std::string HexT(const trace_api::TraceId& id) {
  char b[32]; id.ToLowerBase16(otel_nostd::span<char, 32>{b}); return std::string(b, 32);
}
std::string HexS(const trace_api::SpanId& id) {
  char b[16]; id.ToLowerBase16(otel_nostd::span<char, 16>{b}); return std::string(b, 16);
}
void AppendAttr(std::string& o, const std::string& k, const otel_common::OwnedAttributeValue& v) {
  o += "{\"key\":\"" + JEsc(k) + "\",\"value\":{";
  if (otel_nostd::holds_alternative<std::string>(v))
    o += "\"stringValue\":\"" + JEsc(otel_nostd::get<std::string>(v)) + "\"";
  else if (otel_nostd::holds_alternative<int64_t>(v))
    o += "\"intValue\":\"" + std::to_string(otel_nostd::get<int64_t>(v)) + "\"";
  else if (otel_nostd::holds_alternative<bool>(v))
    o += std::string("\"boolValue\":") + (otel_nostd::get<bool>(v) ? "true" : "false");
  else if (otel_nostd::holds_alternative<double>(v))
    o += "\"doubleValue\":" + std::to_string(otel_nostd::get<double>(v));
  else
    o += "\"stringValue\":\"<unsupported>\"";
  o += "}}";
}

class OtlpHttpJson final : public trace_sdk::SpanExporter {
 public:
  explicit OtlpHttpJson(std::string url) : url_(std::move(url)) {}
  std::unique_ptr<trace_sdk::Recordable> MakeRecordable() noexcept override {
    return std::unique_ptr<trace_sdk::Recordable>(new trace_sdk::SpanData());
  }
  otel_common::ExportResult Export(
      const otel_nostd::span<std::unique_ptr<trace_sdk::Recordable>>& recs) noexcept override {
    if (recs.empty()) return otel_common::ExportResult::kSuccess;
    auto* first = static_cast<trace_sdk::SpanData*>(recs[0].get());
    std::string ra;
    for (const auto& kv : first->GetResource().GetAttributes()) {
      if (!ra.empty()) ra += ",";
      AppendAttr(ra, kv.first, kv.second);
    }
    std::string sj;
    for (auto& rec : recs) {
      auto* sd = static_cast<trace_sdk::SpanData*>(rec.get());
      if (!sj.empty()) sj += ",";
      uint64_t st = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        sd->GetStartTime().time_since_epoch()).count();
      uint64_t en = st + std::chrono::duration_cast<std::chrono::nanoseconds>(sd->GetDuration()).count();
      sj += "{\"traceId\":\"" + HexT(sd->GetTraceId()) + "\",\"spanId\":\"" + HexS(sd->GetSpanId()) + "\",";
      if (sd->GetParentSpanId().IsValid()) sj += "\"parentSpanId\":\"" + HexS(sd->GetParentSpanId()) + "\",";
      sj += "\"name\":\"" + JEsc(std::string(sd->GetName())) + "\",";
      sj += "\"kind\":" + std::to_string((int)sd->GetSpanKind() + 1) + ",";
      sj += "\"startTimeUnixNano\":\"" + std::to_string(st) + "\",\"endTimeUnixNano\":\"" + std::to_string(en) + "\"";
      std::string at;
      for (const auto& kv : sd->GetAttributes()) { if (!at.empty()) at += ","; AppendAttr(at, kv.first, kv.second); }
      if (!at.empty()) sj += ",\"attributes\":[" + at + "]";
      sj += "}";
    }
    std::string body = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[" + ra +
                       "]},\"scopeSpans\":[{\"scope\":{\"name\":\"mooncake\"},\"spans\":[" + sj + "]}]}]}";
    CURL* c = curl_easy_init();
    if (c) {
      curl_slist* h = curl_slist_append(nullptr, "Content-Type: application/json");
      curl_easy_setopt(c, CURLOPT_URL, url_.c_str());
      curl_easy_setopt(c, CURLOPT_HTTPHEADER, h);
      curl_easy_setopt(c, CURLOPT_POSTFIELDS, body.c_str());
      curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)body.size());
      curl_easy_setopt(c, CURLOPT_TIMEOUT, 5L);
      curl_easy_setopt(c, CURLOPT_WRITEFUNCTION,
                       +[](char*, size_t s, size_t n, void*) -> size_t { return s * n; });
      curl_easy_perform(c);
      curl_slist_free_all(h);
      curl_easy_cleanup(c);
    }
    return otel_common::ExportResult::kSuccess;
  }
  bool ForceFlush(std::chrono::microseconds = std::chrono::microseconds::max()) noexcept override { return true; }
  bool Shutdown(std::chrono::microseconds = std::chrono::microseconds::max()) noexcept override { return true; }

 private:
  std::string url_;
};

// Read the traceparent from the current coro_rpc request attachment (handlers
// are synchronous, so the thread-local context is valid for the call).
std::string_view CurrentAttachment() {
  auto*& ctx = coro_rpc::detail::set_context<coro_rpc::protocol::coro_rpc_protocol>();
  if (ctx) return ctx->get_request_attachment();
  return {};
}

}  // namespace

void InitTracing(const std::string& service_name, const std::string& endpoint) {
  if (endpoint.empty()) return;
  curl_global_init(CURL_GLOBAL_ALL);
  std::unique_ptr<trace_sdk::SpanExporter> exporter(new OtlpHttpJson(endpoint));
  // NOTE: SimpleSpanProcessor (sync POST per span) for bring-up; switch to
  // BatchSpanProcessor + sampling for production.
  auto processor = trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));
  auto resource = otel_resource::Resource::Create({{"service.name", service_name}});
  std::shared_ptr<trace_api::TracerProvider> provider =
      trace_sdk::TracerProviderFactory::Create(std::move(processor), resource);
  trace_api::Provider::SetTracerProvider(provider);
  g_enabled.store(true, std::memory_order_relaxed);
}

bool Enabled() { return g_enabled.load(std::memory_order_relaxed); }

// ----- ServerSpanScope ------------------------------------------------------
struct ServerSpanScope::Impl {
  otel_nostd::shared_ptr<trace_api::Span> span;
  // Activates `span` as the thread-local active span so child Step spans nest
  // under it. Safe because Mooncake RPC handlers run synchronously (no co_await
  // hops between this scope and the steps created inside the handler).
  std::unique_ptr<trace_api::Scope> scope;
};
ServerSpanScope::ServerSpanScope(std::string_view rpc_name) {
  if (!g_enabled.load(std::memory_order_relaxed)) return;
  impl_ = std::make_unique<Impl>();
  trace_api::StartSpanOptions o;
  o.parent = Extract(CurrentAttachment());
  o.kind = trace_api::SpanKind::kServer;
  impl_->span = Tracer()->StartSpan(NS(rpc_name), o);
  impl_->scope = std::make_unique<trace_api::Scope>(impl_->span);
}
ServerSpanScope::~ServerSpanScope() {
  if (impl_ && impl_->span) impl_->span->End();
}

struct ServerSpanScope::Step::Impl {
  otel_nostd::shared_ptr<trace_api::Span> span;
};
ServerSpanScope::Step::Step(std::string_view name) {
  if (!g_enabled.load(std::memory_order_relaxed)) return;
  impl_ = std::make_unique<Impl>();
  // Parent the sub-step to the current active span via thread-local context;
  // safe here because Mooncake handlers run synchronously.
  impl_->span = Tracer()->StartSpan(NS(name));
}
ServerSpanScope::Step::~Step() {
  if (impl_ && impl_->span) impl_->span->End();
}

// ----- ClientSpanScope ------------------------------------------------------
struct ClientSpanScope::Impl {
  otel_nostd::shared_ptr<trace_api::Span> span;
};
ClientSpanScope::ClientSpanScope(std::string_view rpc_name) {
  if (!g_enabled.load(std::memory_order_relaxed)) return;
  impl_ = std::make_unique<Impl>();
  impl_->span = Tracer()->StartSpan(NS(rpc_name));
  traceparent_ = Inject(impl_->span->GetContext());
}
ClientSpanScope::~ClientSpanScope() {
  if (impl_ && impl_->span) impl_->span->End();
}

// ----- OpScope (in-process operation span) ----------------------------------
struct OpScope::Impl {
  otel_nostd::shared_ptr<trace_api::Span> span;
  std::unique_ptr<trace_api::Scope> scope;
};
OpScope::OpScope(std::string_view name) {
  if (!g_enabled.load(std::memory_order_relaxed)) return;
  impl_ = std::make_unique<Impl>();
  impl_->span = Tracer()->StartSpan(NS(name));  // child of current active span
  impl_->scope = std::make_unique<trace_api::Scope>(impl_->span);
}
OpScope::~OpScope() {
  if (impl_ && impl_->span) impl_->span->End();
}

}  // namespace mooncake::tracing

#endif  // WITH_TRACING
