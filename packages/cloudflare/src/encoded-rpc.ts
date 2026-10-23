/**
 * Calls across a Durable Object boundary carry encoded values, because Cloudflare RPC rejects
 * class instances such as `Option` and `DateTime` (engine-design.md decision 20). Both sides are
 * our own code, so a value that fails its schema is a bug: a defect, not a typed failure.
 */
import { Effect, Schema } from 'effect';

/** The caller's side: encodes `value`, sends it with `send`, and decodes the reply. */
export const sendEncoded = <Request, RequestEncoded, Result, ResultEncoded, E>({
    request,
    result,
    value,
    send,
}: {
    readonly request: Schema.Codec<Request, RequestEncoded>;
    readonly result: Schema.Codec<Result, ResultEncoded>;
    readonly value: Request;
    readonly send: (encoded: RequestEncoded) => Effect.Effect<ResultEncoded, E>;
}): Effect.Effect<Result, E> =>
    Schema.encodeEffect(request)(value).pipe(
        Effect.orDie,
        Effect.flatMap(send),
        Effect.flatMap((encoded) => Effect.orDie(Schema.decodeEffect(result)(encoded))),
    );

/** The object's side: an operation that decodes its request and encodes its result. */
export const serveEncoded =
    <Request, RequestEncoded, Result, ResultEncoded, E>({
        request,
        result,
        operation,
    }: {
        readonly request: Schema.Codec<Request, RequestEncoded>;
        readonly result: Schema.Codec<Result, ResultEncoded>;
        readonly operation: (request: Request) => Effect.Effect<Result, E>;
    }) =>
    (encoded: RequestEncoded): Effect.Effect<ResultEncoded, E> =>
        Schema.decodeEffect(request)(encoded).pipe(
            Effect.orDie,
            Effect.flatMap(operation),
            Effect.flatMap((value) => Effect.orDie(Schema.encodeEffect(result)(value))),
        );
