---
name: effect-program-design
description: >-
  Design, write, review, and test this repo's Effect v4 programs: the durable streams HttpApi, the stream store
  service and its memory/SQLite layers, Durable Object hosting, auth middleware, the Effect client, schemas, tagged
  failures, resource ownership, concurrency, and real-seam Effect tests. Use for any Effect service, adapter, or
  Effect-based code review in this repo.
---

# Effect Program Design

Build deep modules: a small, protocol-shaped public interface hides storage, offset encoding, producer state, fork
bookkeeping, waiter wake-up, Durable Object routing, and auth. Keep expected failures in Effect's error channel,
dependencies in `R`, resources in scopes, and external values at explicit schema boundaries.

This repo is an Effect v4 Bun monorepo built with Vite+. Read the installed declarations first; when they do not
settle an API, read `../effect` (Effect source) and `../alchemy-effect`, not Effect v3 docs or examples. The protocol
contract is `../durable-streams/PROTOCOL.md`; the conformance suite is the acceptance test.

## Architecture

- **The HTTP protocol is the public interface; storage is layer-native.** Clients see `PROTOCOL.md` over `HttpApi`.
  Handlers depend on a stream store service; hosts choose the in-memory or Durable Object SQLite layer at the
  composition root with `Layer`, never through a handler or request option.
- **One Durable Object per stream.** The Worker routes a stream path to its DO. The DO owns that stream's SQLite
  data, producer state, waiters, and TTL alarm. Do not share mutable state between streams.
- **The DO runs one request at a time between awaits.** Validate-then-write must run as synchronous SQL with no
  `await`/async `yield*` between the check and the write. Long-poll and SSE waits park on in-memory waiters, never
  on a loop that holds the thread, so heavy writes cannot starve reads and the reverse.
- **Auth is a middleware layer.** Protect the API with `HttpApiMiddleware` and `HttpApiSecurity`; the backend
  (for example `@yielded/auth`) is a layer the host supplies. Handlers never parse tokens.
- **Protocol values are schema contracts.** Model headers, offsets, producer fields, and stored records with
  `Schema`; decode before dispatching. Offsets are opaque and sort as strings.
- **The client mirrors the server seam.** The Effect client is a `Context.Service` over `HttpClient`; auth plugs in
  as a layer, not as an argument to each call.

## The defaults

1. **Deep modules.** A module's interface is the cost; hidden behavior is the benefit. A caller supplies the domain
   values it holds (stream path, offset, body, producer id), not SQL clients, DO stubs, layers, or rows.
2. **Effects keep their channels.** Expected failure stays in `E`; dependencies stay in `R`; resources have an
   owning scope. Do not pass errors, services, layers, or effects around as data to compose them later.
3. **Typed failures recover in-channel.** Classify an untrusted or throwing boundary once, then use
   `Effect.catchTag` / `Effect.catchTags`. Never recover with `instanceof` or `_tag` checks.
4. **Normal tagged data dispatches through Effect.** Use `Predicate` for reusable narrowing and `Match` for complete
   transformations. Lint bans manual `_tag` comparison. See `PREDICATE-MATCH.md`.
5. **Schemas parse boundaries.** Decode request headers, bodies, query params, SQLite rows, DO RPC payloads, and
   client responses at their edge. Do not cast or hand-narrow unknown data in the core.
6. **Tests cross real seams.** Test services through their public interface with `@effect/vitest` and swap layers or
   real test adapters. No `vi.mock`, module patching, or spies.

## Module depth and seams

- Give public operations named domain inputs and outputs. The store's `append` takes a stream and a body and returns
  the new tail offset; it hides producer dedupe, sequence checks, and waiter wake-up.
- Use the deletion test: deleting a useful module should spread its rules across callers, not just remove a
  pass-through.
- Keep pure protocol decisions (offset math, conflict order, JSON flattening, cursor choice, header building) apart
  from the I/O shell. Pure helpers take and return domain values; layers sequence Effects and own I/O.
- Do not add a wrapper only to mirror an SDK or the DO API. A module earns its seam by owning lifecycle, policy,
  parsing, or coordination.
- Runtime-varying capabilities belong in Effect's environment; plain values the caller holds stay explicit inputs.

## Services, layers, and resources

- Prefer `Context.Service` for a capability and `Layer.effect` / `Layer.scoped` for its implementation. Small
  services may keep shape, tag, and live layer in one file; split storage, transport, and coordination only when the
  split improves locality.
- Yield dependencies where an operation needs them rather than forwarding them through public signatures. Lint bans
  `Effect.serviceOption`; make a dependency required or give it a layer.
- Provide each layer once at the composition root (Worker entry, DO class, test). The in-memory and SQLite stores
  must pass the same behavior tests.
- Timers and fibers started by a Worker request die with it. Keep long-poll waits inside the request that owns them;
  use DO alarms for work that must outlive a request.
- Acquire resources in a scope; supervise background work; bound concurrency for fan-out. Respect platform limits
  (SQLite value size, response read budget) as explicit constants with tests.
- Use `Effect.fn("streams.operation")` or `Effect.withSpan` on public and I/O boundaries. Never log tokens, secrets,
  or stream payloads.

## Errors and boundaries

- Model expected failures as tagged errors with fields a caller can use. Errors that map to an HTTP status cross an
  encoded boundary: use a schema-backed tagged error annotated with its status. Use `Data.TaggedError` for
  internal-only failures.
- Wrap a throwing SDK or platform API once at its adapter edge with `Effect.try` / `Effect.tryPromise`, mapping the
  cause into the module's error vocabulary.
- Keep rich internal failures until the module boundary, then narrow to the outcomes callers act on: the protocol's
  status codes. A fallback is correct only when the protocol requires it.
- Storage failures are defects unless the protocol names a response for them. Do not fall back to an empty stream.
- Use `catchCause` only for a deliberate top-level safety net, and log the cause before swallowing it.

## Schema and domain data

- Use `Schema.Struct` for records and `Schema.TaggedStruct` plus `Schema.Union` for encoded tagged variants. Derive
  the type with `typeof X.Type`.
- Use `Schema.Literals` for closed vocabularies. Brand offsets and stream paths, where mix-ups with plain strings are
  likely; do not brand by default.
- Use `Schema.optionalKey`, `Schema.optional`, and `Schema.NullOr` to keep absent, `undefined`, and `null` distinct.
  Absent headers and empty headers mean different things in the protocol.
- Decode untrusted values with `Schema.decodeUnknownEffect`; use `make` / `makeEffect` only for trusted construction.
- Keep stored SQLite schemas backward compatible. Add a migration and upcast at decode rather than changing what an
  existing column means.

Read `SCHEMA-DOMAIN-PATTERNS.md` for schema, stored-data, and error-model choices.

## Predicate and Match

- Use `Effect.catchTag` / `Effect.catchTags` for typed errors in `Effect<A, E, R>`.
- Use `Predicate.isTagged` for a reusable one-tag guard or a named multi-tag refinement for `find`, `filter`, or a
  guard clause.
- Use `Match.type<T>()` with `Match.tagsExhaustive` for a reusable transformation over a repo-owned closed union, and
  `Match.value(value)` for a one-off dispatch.
- Neither `Predicate` nor `Match` validates unknown input. Decode it first.

Read `PREDICATE-MATCH.md` before writing or reviewing tagged control flow.

## Tests

- Use `@effect/vitest` and `it.effect`; provide the layer graph to the program under test.
- Run store behavior tests against both the in-memory and SQLite layers. Use the real `@effect/sql-sqlite-do` or
  workerd runtime when behavior depends on SQLite or DO semantics.
- Use `TestClock`, `Deferred`, `Queue`, `Latch`, and `Ref` instead of sleeps. Long-poll and TTL tests must be
  deterministic in unit tests; the conformance suite covers real timing.
- Assert the returned value or error and the observable end state: stored messages, headers, offsets, woken waiters.
- Fakes fail loudly on unexpected calls. If a dependency cannot be swapped at a layer seam, fix the module boundary.
- Run `@durable-streams/server-conformance-tests` against the server before calling protocol work done.

## References

- `PREDICATE-MATCH.md` for tagged normal-value dispatch.
- `SCHEMA-DOMAIN-PATTERNS.md` for schema, stored-data, and error-model choices.
- `REFERENCE.md` for this repo's module map and the expected service/layer shapes.
- `references/` for deeper topic guides: `STREAMS.md`, `HTTP_CLIENTS.md`, `TESTING.md`, `RESOURCES_CONCURRENCY.md`,
  `SCHEDULING_WORKERS.md`, `CONFIG_SECRETS.md`, `PLATFORM.md`, and others. Read the ones the task touches. Where
  they conflict with this file, this file wins.
- `codebase-design` for shared vocabulary on depth, interface, seam, adapter, leverage, and locality.

## Review checklist

- [ ] The public interface is small, protocol-shaped, and hides storage, routing, and auth wiring.
- [ ] Expected failures stay typed in `E`; dependencies stay in `R`; resources have an owner.
- [ ] Unknown data is decoded at the edge; raw rows and DO values do not leak through a public contract.
- [ ] Typed errors use `catchTag` / `catchTags`; trusted tagged values use `Predicate` / `Match`.
- [ ] Repo-owned closed unions use an exhaustive transformation where a new variant must force review.
- [ ] Check-then-write in a DO has no await between check and write; waits never block the thread.
- [ ] Concurrency, retries, size limits, and resource lifetime are bounded and owned by the module that needs them.
- [ ] Tests use `@effect/vitest`, real seams, deterministic coordination, and no module mocks or spies.
- [ ] Both store layers pass the shared behavior tests; protocol changes pass the conformance suite.
