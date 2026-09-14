# Operation-wide publication batches

## Contract

Every persistent outbox publication is part of a **source-operation
publication**: all events published to one table namespace during one atomic
operation — across publish calls, persist chunks, post-persist/repost hooks,
and re-entrant commit-hook generations — commit as one indivisible unit, and
downstream consumers can only ever observe that unit whole.

Ordinary publish calls remain independent and unchanged. Obix accumulates the
operation's publications on its persist hook; es-entity's commit-finalization
phase seals the boundary only after every ordinary hook has finished. A
finalizer registering any new hook aborts the whole source transaction rather
than sealing an incomplete publication. Requires the companion es-entity
`CommitHook::is_finalizer` extension (pinned below).

Publishing onto an operation without commit-hook support (a bare
`sqlx::Transaction`) fails explicitly, before writing anything. Empty
operations create no publication.

## Storage and ordering

Payloads are stored **once**, in the existing persistent event table. Two
small tables complete the model:

- `<events>_batch_head` — one transactional row. The first persist chunk of an
  operation reserves a contiguous position range by advancing it; the row lock
  is held until the operation commits or rolls back.
- `<events>_batches` — `(first_sequence, last_sequence)` boundaries, inserted
  at finalization in the same transaction.

Position reservation and the events share the source transaction: rollback
restores both, so the stream has **no sequence holes and no placeholder
rows**. Aborted operations leave nothing behind — the gap-filling,
abandonment-proofing, and placeholder-compensation machinery that the
pre-allocation `BIGSERIAL` design required is gone.

The head lock is a deliberate **single-writer serialization point** per
namespace: one source operation's publications commit in order, and no
operation can interleave positions inside another's range. No throughput
claims are made.

## Consumers

- `load_publication_batches(after, limit)` returns up to `limit` **complete
  publications**, loading member payloads from the event table. The cursor
  must be `BEGIN` or a previously delivered `last_sequence()`; an interior
  cursor, a missing prefix, or a publication whose member rows are missing or
  out of place fails closed. A `limit` counts publications, never events.
- `EventSubscription::PublicationBatches` handlers receive one whole
  publication per callback via `handle_publication_batch`; the job checkpoint
  advances only after the callback's transaction commits. Event-level
  collect/defer/isolate policies and `max_batch_size` never split a
  publication. A foreign sink must apply the publication atomically and
  tolerate replay after its own commit but before the source checkpoint.
- Event-level listeners and cursors share the same committed stream and
  positions. `highest_known_persistent_sequence` is the committed head — it
  excludes in-flight operations, unlike the old allocator frontier.
- `await_caught_up` fences **committed** work. It is a visibility barrier for
  atomic sinks; fencing business operations still in flight requires producer
  quiescence, not a head read.

## Local development

```sh
DATABASE_URL=<postgres-url> cargo sqlx migrate run
PG_CON=<postgres-url> SQLX_OFFLINE=true cargo nextest run
```

Companion es-entity tests: `cargo nextest run --test finalizers` in
[es-entity #226](https://github.com/GaloyMoney/es-entity/pull/226), pinned by
immutable Git revision in `Cargo.toml`. Downstream Cargo roots must repeat the
patch; dependency-local patches are not inherited.

## Not production-ready

- Existing migrations are edited in place; there is intentionally no upgrade
  path (nothing deployed depends on this yet).
- One head lock serializes all writers per namespace for their whole
  transaction. Not benchmarked; multiple namespaces in one operation need a
  canonical seal-lock order before production use.
- The publication runner polls every 100 ms and applies one publication per
  source operation. Coalescing and notifications are future work.
- No retention/archival story for the boundaries table; no chunk framing for
  pathologically large single publications (they are delivered whole, in
  memory).
