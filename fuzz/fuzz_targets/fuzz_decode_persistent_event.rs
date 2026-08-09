#![no_main]

//! Coverage-guided fuzz target for `obix::decode_persistent_event`.
//!
//! `decode_persistent_event` is the single chokepoint through which every
//! persistent outbox row read back from Postgres is decoded into the
//! consumer's event type. The persisted payload column is the most untrusted
//! data obix reads: a row can be written by a different (future, or
//! co-tenant) event enum sharing the table, by a hand-edit, or by a
//! migration gone wrong. Its contract is explicit — see the doc on
//! `decode_persistent_event` — that a poison row must NEVER panic (a single
//! poison row once wedged the whole pipeline in a hot panic/retry loop); a
//! row that doesn't decode becomes an honest `Err(UndecodableEventError)`
//! that still occupies its sequence position, its fate decided by consumer
//! policy.
//!
//! So we feed arbitrary JSON values through it and assert:
//!   1. it never panics (libFuzzer reports any panic as a crash), AND
//!   2. on the `Err` arm, the carried `failure.raw` is the exact input value
//!      (not a lossy/truncated copy) and the serde `error` string is
//!      non-empty — the same "honest copy" property the es-entity
//!      constraint-detail fuzzer asserts for its parser.
//!
//! This is the obix analog of es-entity's `fuzz_event_hydration` target.
//! Pure — no Postgres needed.

use libfuzzer_sys::fuzz_target;

use chrono::Utc;
use obix::out::OutboxEventId;
use obix::{EventSequence, UndecodableEventError, decode_persistent_event};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A representative consumer payload: a few variants exercising different
/// decode paths (struct / newtype / unit). Arbitrary JSON that doesn't match
/// any variant lands in the `Err` arm, exercising the poison-row path that
/// previously panicked the pipeline.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum FuzzPayload {
    Created { name: String, count: u64 },
    Updated(String),
    Deleted,
}

fuzz_target!(|data: &[u8]| {
    // Two input shapes share the fuzzer:
    //   - valid JSON  -> the realistic poison-row shape (a row written by a
    //     different event enum, a hand-edit, …), fed straight in;
    //   - invalid JSON -> exercise the `None`-placeholder path (a NULLed or
    //     gap-filled row), which must be a TOTAL function — always `Ok` with
    //     `payload: None`, regardless of the surrounding bytes.
    let (payload, must_be_ok_with_none) = match serde_json::from_slice::<serde_json::Value>(data) {
        Ok(value) => (Some(value), false),
        Err(_) => (None, true),
    };

    let id = OutboxEventId::from(Uuid::nil());
    let sequence = 0u64;
    let recorded_at = Utc::now();

    match decode_persistent_event::<FuzzPayload>(
        id,
        sequence,
        recorded_at,
        None,
        payload.clone(),
    ) {
        Ok(event) => {
            if must_be_ok_with_none {
                // The `None`-placeholder path is always `Ok` with no payload.
                assert!(
                    event.payload.is_none(),
                    "non-JSON bytes yielded a decoded payload on the placeholder path"
                );
                assert!(payload.is_none());
            } else if let Some(ref raw) = payload {
                // `Ok` with a real payload => serde_json must agree, byte for
                // byte. The library is not allowed to "succeed" on something
                // serde itself rejects, nor reject something serde accepts.
                let direct = FuzzPayload::deserialize(raw)
                    .expect("decode_persistent_event Ok'd a value serde_json rejects");
                assert_eq!(event.payload.as_ref().unwrap(), &direct);
            }
            // Row metadata round-trips onto the `Ok` arm unchanged.
            assert_eq!(event.id, id);
            assert_eq!(u64::from(event.sequence), sequence);
            assert_eq!(event.recorded_at, recorded_at);
            assert!(event.tracing_context.is_none());
        }
        Err(UndecodableEventError {
            id: err_id,
            sequence: err_seq,
            recorded_at: err_ts,
            failure,
        }) => {
            // `None` can never be undecodable — that's the placeholder path.
            let raw = payload
                .as_ref()
                .expect("None payload was reported undecodable");
            // Honest copy: the raw carried on the error is the exact input,
            // not a lossy/truncated reconstruction.
            assert_eq!(&failure.raw, raw, "failure.raw must equal the input value");
            // The serde error message is populated.
            assert!(!failure.error.is_empty(), "serde error string is empty");
            // Row metadata round-trips onto the `Err` arm unchanged.
            assert_eq!(err_id, id);
            assert_eq!(err_seq, EventSequence::from(sequence));
            assert_eq!(err_ts, recorded_at);
        }
    }
});
