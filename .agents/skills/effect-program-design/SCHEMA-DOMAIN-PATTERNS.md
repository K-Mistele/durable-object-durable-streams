# Schema and Domain Patterns

Schemas define this repo's encoded boundaries: protocol headers and bodies, stored SQLite records, DO RPC payloads,
and client responses. Use the smallest model that keeps the contract; do not add schema ceremony to trusted local
control flow with no encoded form.

## Schema chooser

| Value role                                    | Default representation                                                          |
| --------------------------------------------- | ------------------------------------------------------------------------------- |
| Ordinary encoded record                       | `Schema.Struct`                                                                 |
| Closed scalar vocabulary                      | `Schema.Literals`                                                               |
| One encoded `_tag` variant                    | `Schema.TaggedStruct`                                                           |
| Encoded tagged union                          | `Schema.Union` of `Schema.TaggedStruct` variants                                |
| Internal-only tagged decision                 | A precise TypeScript union or `Data.TaggedEnum` when constructors/matchers help |
| Internal-only expected failure                | `Data.TaggedError`                                                              |
| Failure returned as an HTTP response          | `Schema.TaggedError` annotated with `httpApiStatus`                             |
| Identity with a real cross-domain mix-up risk | A constrained branded schema                                                    |

`Schema.TaggedClass` is not a default. Use it only when class identity or behavior has a real need.

## Protocol values

`PROTOCOL.md` is the source of truth. Model each header and query parameter with the schema that matches its wire
form, and decode once at the handler edge.

```ts
import { Schema } from "effect";

export const Offset = Schema.String.pipe(Schema.brand("Offset"));
export type Offset = typeof Offset.Type;

export const ReadStart = Schema.Union([Schema.Literal("-1"), Schema.Literal("now"), Offset]);

export const ProducerHeaders = Schema.Struct({
  "producer-id": Schema.String,
  "producer-epoch": Schema.NumberFromString,
  "producer-seq": Schema.NumberFromString,
});
```

Offsets are opaque to clients and must sort as strings. Keep offset encoding and parsing in one module; nothing else
builds or splits an offset string.

## Stored records

Decode SQLite rows at the storage adapter. Do not let raw rows or column names cross the store interface.

When a stored format changes, add a migration and upcast at decode. Do not change the meaning of an existing column
or use a type assertion to reread old data.

## Optionality and unknown fields

Choose the exact wire contract:

- `Schema.optionalKey(S)` means a key may be absent.
- `Schema.optional(S)` allows an absent key or an explicit `undefined`.
- `Schema.NullOr(S)` means the key is present and its value is `null` or `S`.
- `Schema.Unknown` and `Schema.Json` fit inside an explicit payload boundary, such as a JSON-mode stream body. Keep
  that data contained; do not branch on its contents.

Do not merge absent, empty, `undefined`, and `null` to make a caller easier to write. A missing header and an empty
header can mean different things in the protocol.

## Decode before domain behavior

```ts
const decodeHeaders = Schema.decodeUnknownEffect(ProducerHeaders);

const producer = yield * decodeHeaders(request.headers);
const offset = Offset.make(encodeOffset(position));
```

Use `Schema.decodeUnknownEffect` for encoded or untrusted input. Use `make` only when the input is already the
schema's type side; use `makeEffect` when trusted construction can still fail. Do not use `as T` to skip decoding.

## Tagged values and errors

After decoding, tagged data uses `Predicate` and `Match` as described in `PREDICATE-MATCH.md`. Decoding proves the
whole variant shape; `Predicate.isTagged` proves only a tag.

Expected errors keep Effect's error channel. Errors the HttpApi turns into responses are schema-backed and carry
their status; errors that never leave a module use `Data.TaggedError`. Recover with `Effect.catchTag` /
`Effect.catchTags`.

## IDs

Brand an identity only when it prevents a likely mix-up or guards a stored or public contract: offsets, stream
paths, producer ids. A raw string is right for content types, freeform text, and display values.

Do not keep an exported hand-written object type that copies an exported schema's fields; derive it from the schema.
