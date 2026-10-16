# Effect Program Design Reference

The repo has no server code yet. This file records the target shape; replace each "planned" entry with a real path
once the module lands, so later work copies a real pattern.

| Concern                  | Reference                                                       | What it shows                                                        |
| ------------------------ | --------------------------------------------------------------- | -------------------------------------------------------------------- |
| Protocol contract        | `../durable-streams/PROTOCOL.md`                                | Status codes, headers, offsets, producers, forks, read modes.        |
| Prior art (non-Effect)   | `../durable-streams/packages/server-cloudflare`                 | DO-per-stream, SQLite tables, waiters, SSE, TTL alarms.              |
| Effect source            | `../effect/packages/effect/src/http-api`, `../effect/packages/sql/sqlite-do` | HttpApi, middleware, security, DO SQLite client.        |
| Alchemy on Cloudflare    | `../alchemy-effect`                                             | Workers, Durable Objects, `alchemy dev`, sync SQLite transactions.   |
| Client                   | `../effect-durable-streams-client`                              | `DurableStreamsClient` over `HttpClient`, producers, SSE, retries.   |
| Stream store service     | planned                                                         | The small capability every handler depends on.                       |
| Memory / SQLite layers   | planned                                                         | Two implementations of one service, one shared behavior test suite.  |
| HttpApi definition       | planned                                                         | Endpoints, schema-backed errors with `httpApiStatus`, raw bodies.    |
| Auth middleware          | planned                                                         | `HttpApiMiddleware` + `HttpApiSecurity`, backend supplied by layer.  |

## Public shape

Two interfaces, on purpose:

1. The HTTP protocol, served by `HttpApi`. Clients and the conformance suite see only this.
2. An internal Effect interface, where `Context.Service` tags and `Layer`s express capabilities, storage choice, and
   test seams.

Data flows one way:

```text
HTTP request -> auth middleware -> HttpApi handler -> Worker routes to stream DO -> stream store -> SQLite
```

Handlers do not know which store layer runs. The DO does not know about auth. Storage does not know about HTTP.

## A small service seam

Use a service when callers need a capability, not a data structure. The service says what callers can do; its layer
owns how.

```ts
import { Context } from "effect";
import type { Effect, Stream } from "effect";

export type StreamStoreService = {
  readonly append: (input: AppendInput) => Effect.Effect<AppendOutcome, AppendError>;
  readonly read: (input: ReadInput) => Effect.Effect<ReadResult, ReadError>;
  readonly live: (input: ReadInput) => Stream.Stream<Chunk, ReadError>;
};

export class StreamStore extends Context.Service<StreamStore, StreamStoreService>()(
  "durable-streams/StreamStore",
) {}
```

The layer coordinates offsets, producer dedupe, closing, TTL, forks, and waiter wake-up. None of those details become
arguments to `append` or `read`.

## Durable Object rules

- Check-then-write runs as one synchronous SQL step. An `await` between check and write lets another request in.
- Read the full request body before any state check.
- Waiters live in DO memory. Wake data waiters before close waiters. A long-poll times out well under the
  conformance suite's 5s test timeout.
- Wrap `@effect/sql-sqlite-do` transactions with a sync scheduler, as alchemy's Drizzle Cloudflare adapter does.
- TTL uses a lazy check on access plus a DO alarm.

## Errors are not normal tagged values

For a typed store or auth failure, keep the error channel and recover in it:

```ts
operation.pipe(
  Effect.catchTags({
    StreamNotFound: () => Effect.succeed(fallback),
    StreamGone: () => Effect.succeed(fallback),
  }),
);
```

For a decoded record or request union, dispatch it as a normal value with `Predicate` or `Match`. Do not move an
Effect error into a normal union to match it.

## Testing at a real seam

Write an `it.effect` program that gets the public service and provides its dependencies as layers. Replace only true
externals: clock, auth backend, network. Use the real SQLite layer when behavior depends on storage.

```ts
it.effect("append returns the new tail offset", () =>
  Effect.gen(function* () {
    const store = yield* StreamStore;
    const outcome = yield* store.append(input);
    const read = yield* store.read({ stream: input.stream, from: "-1" });
    expect(read.nextOffset).toBe(outcome.offset);
  }).pipe(Effect.provide(storeLayer)),
);
```

No `vi.mock`, `vi.spyOn`, module patching, or sleeps. If behavior cannot be tested by providing a layer, move the
seam rather than patching the module.
