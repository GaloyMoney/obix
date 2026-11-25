#![cfg(feature = "tracing")]

use es_entity::context::TracingContext;
use obix::out::{EphemeralOutboxEvent, PersistentOutboxEvent};
use opentelemetry::trace::{SpanId, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_subscriber::layer::SubscriberExt as _;

fn producer_context() -> TracingContext {
    TracingContext {
        trace_id: "11111111111111111111111111111111".into(),
        span_id: "2222222222222222".into(),
        trace_flags: 1,
        traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
        tracestate: "galoy=retain,vendor=state".into(),
    }
}

fn capture(run: impl FnOnce()) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("obix-context-test")));
    tracing::subscriber::with_default(subscriber, run);
    provider.force_flush().expect("flush spans");
    let spans = exporter.get_finished_spans().expect("finished spans");
    provider.shutdown().expect("shutdown tracer provider");
    spans
}

fn assert_producer_parent(span: &SpanData) {
    assert_eq!(span.parent_span_id.to_string(), "2222222222222222");
    assert_eq!(
        span.span_context.trace_id().to_string(),
        "11111111111111111111111111111111"
    );
    assert_eq!(span.span_context.trace_state().get("galoy"), Some("retain"));
    assert_eq!(span.span_context.trace_state().get("vendor"), Some("state"));
}

fn finished_span<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("missing {name} span"))
}

#[test]
fn persistent_event_applies_parent_before_activation() {
    let event = PersistentOutboxEvent {
        id: uuid::Uuid::from_u128(1).into(),
        sequence: 1_u64.into(),
        payload: Some(true),
        tracing_context: Some(producer_context()),
        event_context: None,
        recorded_at: chrono::Utc::now(),
        commit_group: 1_i64.into(),
    };
    let spans = capture(|| {
        let consumer = tracing::info_span!(parent: None, "persistent-consumer");
        event.apply_trace_parent(&consumer);
        consumer.in_scope(|| {});
    });
    let span = finished_span(&spans, "persistent-consumer");
    assert_producer_parent(span);
    assert_ne!(span.span_context.span_id(), SpanId::INVALID);
}

#[test]
fn ephemeral_event_applies_parent_before_activation() {
    let event = EphemeralOutboxEvent {
        event_type: obix::EphemeralEventType::new("test.event"),
        payload: true,
        tracing_context: Some(producer_context()),
        recorded_at: chrono::Utc::now(),
    };
    let spans = capture(|| {
        let consumer = tracing::info_span!(parent: None, "ephemeral-consumer");
        event.apply_trace_parent(&consumer);
        consumer.in_scope(|| {});
    });
    assert_producer_parent(finished_span(&spans, "ephemeral-consumer"));
}
