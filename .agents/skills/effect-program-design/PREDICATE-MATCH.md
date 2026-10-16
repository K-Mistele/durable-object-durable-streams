# Predicate and Match

Use this reference for ordinary tagged values: stored records, decoded request variants, read modes, producer
outcomes, and in-process decisions. It does not replace typed Effect error handling.

## Choose the right construct

| Situation                                          | Use                                                  | Why                                                                   |
| -------------------------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------- |
| Recover from a tagged failure in `Effect<A, E, R>` | `Effect.catchTag` / `Effect.catchTags`               | The error stays in the error channel.                                 |
| Test one tag or create a reusable narrowing filter | `Predicate.isTagged`                                 | Produces a guard that works with `find`, `filter`, and guard clauses. |
| Reuse a multi-tag narrowing filter                 | Compose `Predicate.isTagged` guards                  | Keeps the narrowed union in one named predicate.                      |
| Transform every member of a closed tagged union    | `Match.type<T>()` with `Match.tagsExhaustive`        | Makes added variants a type error at the transformation.              |
| Dispatch a closed union at one call site           | `Match.value(value)`                                 | Keeps the cases and their result together.                            |
| Handle only selected tags on purpose               | `Match.tag` / `Match.tags` plus an explicit fallback | Shows that unmatched values are expected.                             |

`Match` selects a branch for an ordinary value. A branch may return an `Effect`, but `Match` neither runs it nor
recovers its failures. Use `catchTag` / `catchTags` after an Effect has failed with a typed error.

Lint (`anti-slop-effect/no-manual-tag-comparison`, `automation/no-switch`) bans `_tag` comparisons and `switch`.
There is no local-loop exception.

## Predicate: narrow, do not decode

Use `Predicate.isTagged` when a caller needs one narrow branch or a named refinement for an array operation. The
value must already be trusted as the union type. `Predicate.isTagged` proves only the `_tag`; it does not check the
other fields.

```ts
import { Predicate } from "effect";

type DataWaiter = Extract<Waiter, { readonly _tag: "LongPoll" | "Sse" }>;

const isDataWaiter: Predicate.Refinement<Waiter, DataWaiter> = Predicate.or(
  Predicate.isTagged("LongPoll"),
  Predicate.isTagged("Sse"),
);

const toWake = waiters.filter(isDataWaiter);
```

For an untrusted value from a request, a SQLite row, a DO RPC payload, or a server response, decode it with the
owning `Schema` first. Do not use `Predicate.isTagged` or a hand-written property ladder in place of decoding.

## Match: transform complete unions

Use `Match.type<T>()` to define a reusable transformation. `Match.tagsExhaustive` is the default for a closed union:
a new variant makes the compiler flag every transformation that needs a case.

```ts
import { Match } from "effect";

const appendStatus = Match.type<AppendOutcome>().pipe(
  Match.tagsExhaustive({
    Appended: () => 204 as const,
    ProducerAccepted: () => 200 as const,
    Duplicate: () => 204 as const,
  }),
);
```

Use `Match.value(value)` for a one-off dispatch, especially when each case has a distinct output and a conditional
chain would repeat the discriminant.

```ts
const readMode = Match.value(query.live).pipe(
  Match.when("long-poll", () => "LongPoll" as const),
  Match.when("sse", () => "Sse" as const),
  Match.orElse(() => "CatchUp" as const),
);
```

Use `Match.orElse` or `Match.option` only when partial handling is intended. Do not add a fallback to silence
exhaustiveness for a union this repo owns.

## Boundaries and errors stay separate

```ts
const decodeRow = Schema.decodeUnknownEffect(StoredMessage);

const recoverRead = read.pipe(
  Effect.catchTags({
    StreamNotFound: () => Effect.succeed(notFoundResponse),
    StreamGone: () => Effect.succeed(goneResponse),
  }),
);
```

The first line decodes an unknown boundary value. The second block recovers typed failures. Neither should become an
ordinary-value `Match`.

## Review checklist

- Is this a normal tagged value or a typed failure? Use `Predicate` / `Match` only for the former.
- Has untrusted data been decoded by the owning schema?
- Does a reusable tag condition deserve a named `Predicate` refinement?
- Does a repo-owned closed union deserve `Match.tagsExhaustive`?
