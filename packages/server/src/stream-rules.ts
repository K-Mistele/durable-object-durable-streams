/**
 * The protocol's rules for creating and appending, with no I/O. A rule that can refuse a request
 * returns an Effect that fails with the protocol error; the rest return plain values.
 * `StreamOperations` runs them inside the storage transaction with one `now` per request, so the
 * expiry check and the timestamps it writes agree.
 */
import { Data, DateTime, Effect, Equivalence, Match, Option, Order } from 'effect';
import {
    mediaTypeOf,
    type ProducerSeq,
    type StreamSeq,
    StreamSeqOrder,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    ContentTypeMismatchError,
    InvalidEpochSeqError,
    InvalidForkOffsetError,
    SequenceGapError,
    StaleEpochError,
    type StreamClosedError,
    StreamConfigConflictError,
    StreamGoneError,
    StreamNotFoundError,
    StreamSeqConflictError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import { advance, zero } from '@kylelayer/effect-durable-streams-protocol/offset';
import {
    decideProducer,
    type ProducerState,
} from '@kylelayer/effect-durable-streams-protocol/producer';
import type { ProducerRequestId, StoredStream } from './stored-stream';
import type { AppendRequest, CreateForkRequest, ForkPoint } from './stream-operation-requests';

/** The settings a PUT must repeat for an existing stream to count as the same stream (PROTOCOL.md 5.1). */
export type StreamConfig = Pick<
    StoredStream,
    'contentType' | 'ttlSeconds' | 'expiresAt' | 'closed'
>;

/** What the append transaction reads before it applies the rules. */
export type StoredAppendState = {
    readonly stream: Option.Option<StoredStream>;
    readonly producer: Option.Option<ProducerState>;
};

/** Where a producer request falls in its producer's sequence, once out-of-sequence requests are rejected. */
export type ProducerSequencePosition = Data.TaggedEnum<{
    NoProducer: {};
    /** Accept it, and save `state` as the producer's new state. */
    NextInSequence: { readonly state: ProducerState };
    /** A retry of a request already accepted; `lastSeq` is the highest seq accepted so far. */
    AlreadyAccepted: { readonly lastSeq: ProducerSeq };
}>;
export const ProducerSequencePosition = Data.taggedEnum<ProducerSequencePosition>();

export type AppendRejection =
    | StreamNotFoundError
    | StreamGoneError
    | StreamClosedError
    | ContentTypeMismatchError
    | StaleEpochError
    | InvalidEpochSeqError
    | SequenceGapError
    | StreamSeqConflictError;

/** When the stream expires: its Expires-At, or its last access plus its TTL, whichever is first. */
export const expiryOf = (stream: StoredStream): Option.Option<DateTime.Utc> => {
    const ttlExpiry = Option.map(stream.ttlSeconds, (seconds) =>
        DateTime.add(stream.lastAccessedAt, { seconds }),
    );
    return Option.match(stream.expiresAt, {
        onNone: () => ttlExpiry,
        onSome: (expiresAt) =>
            Option.some(
                Option.match(ttlExpiry, {
                    onNone: () => expiresAt,
                    onSome: (ttl) => DateTime.min(expiresAt, ttl),
                }),
            ),
    });
};

export const isExpired = ({
    stream,
    now,
}: {
    readonly stream: StoredStream;
    readonly now: DateTime.Utc;
}): boolean =>
    Option.exists(expiryOf(stream), (expiry) => DateTime.isGreaterThanOrEqualTo(now, expiry));

/** What a stored row means for a request to its stream. */
export type StoredStreamStatus = Data.TaggedEnum<{
    /** Nothing stored, or expired with no forks: the stream no longer exists. */
    Missing: {};
    /** A fork still being created (PROTOCOL.md 4.2 has no state for it; it answers 404). */
    Copying: { readonly stream: StoredStream };
    /** Soft-deleted, or expired while forks still copy from it: 410. */
    Gone: { readonly stream: StoredStream };
    Live: { readonly stream: StoredStream };
}>;
export const StoredStreamStatus = Data.taggedEnum<StoredStreamStatus>();

export const statusOf = ({
    stored,
    now,
}: {
    readonly stored: Option.Option<StoredStream>;
    readonly now: DateTime.Utc;
}): StoredStreamStatus => {
    if (Option.isNone(stored)) {
        return StoredStreamStatus.Missing();
    }
    const stream = stored.value;
    if (Option.isSome(stream.copy)) {
        return StoredStreamStatus.Copying({ stream });
    }
    const expired = isExpired({ stream, now });
    if (stream.softDeleted || (expired && stream.hasForks)) {
        return StoredStreamStatus.Gone({ stream });
    }
    if (expired) {
        return StoredStreamStatus.Missing();
    }
    return StoredStreamStatus.Live({ stream });
};

/** The stream a request can use: 404 when it doesn't exist (or is a fork still copying), 410 when soft-deleted. */
export const getExistingStream = ({
    stored,
    now,
}: {
    readonly stored: Option.Option<StoredStream>;
    readonly now: DateTime.Utc;
}): Effect.Effect<StoredStream, StreamNotFoundError | StreamGoneError> =>
    StoredStreamStatus.$match(statusOf({ stored, now }), {
        Missing: () => Effect.fail(StreamNotFoundError.make({})),
        Copying: () => Effect.fail(StreamNotFoundError.make({})),
        Gone: () => Effect.fail(StreamGoneError.make({})),
        Live: ({ stream }) => Effect.succeed(stream),
    });

export const hasSameConfig = ({
    stream,
    config,
}: {
    readonly stream: StoredStream;
    readonly config: StreamConfig;
}): boolean => {
    const sameMediaType = mediaTypeOf(stream.contentType) === mediaTypeOf(config.contentType);
    const sameTtl = Option.makeEquivalence(Equivalence.Number)(
        stream.ttlSeconds,
        config.ttlSeconds,
    );
    const sameExpiresAt = Option.makeEquivalence(DateTime.Equivalence)(
        stream.expiresAt,
        config.expiresAt,
    );
    const sameClosed = stream.closed === config.closed;
    return sameMediaType && sameTtl && sameExpiresAt && sameClosed;
};

/** PROTOCOL.md 5.1: a PUT on an existing stream with a different config gets 409. A fork's config includes its source, so a plain PUT on a fork conflicts. */
export const rejectConfigConflict = ({
    existing,
    requested,
}: {
    readonly existing: StoredStream;
    readonly requested: StreamConfig;
}): Effect.Effect<void, StreamConfigConflictError> => {
    if (Option.isSome(existing.fork) || !hasSameConfig({ stream: existing, config: requested })) {
        return Effect.fail(StreamConfigConflictError.make({}));
    }
    return Effect.void;
};

export const buildNewStream = ({
    config,
    generation,
    messageCount,
    now,
}: {
    readonly config: StreamConfig;
    readonly generation: number;
    readonly messageCount: number;
    readonly now: DateTime.Utc;
}): StoredStream => ({
    ...config,
    generation,
    tail: advance(zero, messageCount),
    closedBy: Option.none(),
    createdAt: now,
    lastAccessedAt: now,
    lastStreamSeq: Option.none(),
    fork: Option.none(),
    copy: Option.none(),
    softDeleted: false,
    hasForks: false,
});

/** On a closed stream: the exact producer request that closed it, or a close without producer headers, is a harmless repeat. */
export const isRepeatedClose = ({
    stream,
    request,
}: {
    readonly stream: StoredStream;
    readonly request: AppendRequest;
}): boolean => {
    const isClosingProducerRequest = Option.getOrElse(
        Option.zipWith(
            stream.closedBy,
            request.producer,
            (closer, producer) =>
                closer.id === producer.id &&
                closer.epoch === producer.epoch &&
                closer.seq === producer.seq,
        ),
        () => false,
    );
    const isPlainClose = Match.valueTags(request, {
        AppendMessages: () => false,
        CloseStream: ({ producer }) => Option.isNone(producer),
    });
    return isClosingProducerRequest || isPlainClose;
};

/** PROTOCOL.md 5.2: a body's content type must match the stream's, by media type. */
export const rejectContentTypeMismatch = ({
    stream,
    contentType,
}: {
    readonly stream: StoredStream;
    readonly contentType: string;
}): Effect.Effect<void, ContentTypeMismatchError> => {
    if (mediaTypeOf(contentType) !== mediaTypeOf(stream.contentType)) {
        return Effect.fail(ContentTypeMismatchError.make({}));
    }
    return Effect.void;
};

/** PROTOCOL.md 5.2.1: 403 for a stale epoch, 400 for a new epoch not starting at 0, 409 for a seq gap. */
export const rejectOutOfSequenceProducerRequest = ({
    storedProducer,
    producer,
}: {
    readonly storedProducer: Option.Option<ProducerState>;
    readonly producer: Option.Option<ProducerRequestId>;
}): Effect.Effect<
    ProducerSequencePosition,
    StaleEpochError | InvalidEpochSeqError | SequenceGapError
> => {
    if (Option.isNone(producer)) {
        return Effect.succeed(ProducerSequencePosition.NoProducer());
    }
    const decision = decideProducer(storedProducer, {
        epoch: producer.value.epoch,
        lastSeq: producer.value.seq,
    });
    return Match.valueTags(decision, {
        Accepted: ({ next }) =>
            Effect.succeed(ProducerSequencePosition.NextInSequence({ state: next })),
        Duplicate: ({ lastSeq }) =>
            Effect.succeed(ProducerSequencePosition.AlreadyAccepted({ lastSeq })),
        StaleEpoch: ({ currentEpoch }) => Effect.fail(StaleEpochError.make({ currentEpoch })),
        InvalidEpochSeq: () => Effect.fail(InvalidEpochSeqError.make({})),
        SequenceGap: ({ expectedSeq, receivedSeq }) =>
            Effect.fail(SequenceGapError.make({ expectedSeq, receivedSeq })),
    });
};

/** PROTOCOL.md 5.2: a Stream-Seq must be greater than the last one accepted. */
export const rejectStreamSeqRegression = ({
    stream,
    streamSeq,
}: {
    readonly stream: StoredStream;
    readonly streamSeq: Option.Option<StreamSeq>;
}): Effect.Effect<void, StreamSeqConflictError> => {
    const isRegression = Option.exists(streamSeq, (seq) =>
        Option.exists(stream.lastStreamSeq, (lastSeq) =>
            Order.isLessThanOrEqualTo(StreamSeqOrder)(seq, lastSeq),
        ),
    );
    if (isRegression) {
        return Effect.fail(StreamSeqConflictError.make({}));
    }
    return Effect.void;
};

export const updateStreamForAppend = ({
    stream,
    request,
    messageCount,
    now,
}: {
    readonly stream: StoredStream;
    readonly request: AppendRequest;
    readonly messageCount: number;
    readonly now: DateTime.Utc;
}): StoredStream => {
    const closesStream = Match.valueTags(request, {
        AppendMessages: ({ closeAfterAppend }) => closeAfterAppend,
        CloseStream: () => true,
    });
    const streamSeq = Match.valueTags(request, {
        AppendMessages: (append) => append.streamSeq,
        CloseStream: () => Option.none<StreamSeq>(),
    });
    return {
        ...stream,
        tail: advance(stream.tail, messageCount),
        closed: stream.closed || closesStream,
        closedBy: closesStream ? request.producer : stream.closedBy,
        lastStreamSeq: Option.orElse(streamSeq, () => stream.lastStreamSeq),
        lastAccessedAt: now,
    };
};

/**
 * PROTOCOL.md 4.2: a fork PUT repeats an existing fork when it names the same source, offset and
 * sub-offset, and any content type, TTL or Expires-At it sends matches the fork's.
 */
export const isSameForkRequest = ({
    stream,
    request,
}: {
    readonly stream: StoredStream;
    readonly request: CreateForkRequest;
}): boolean => {
    const sameFork = Option.exists(
        stream.fork,
        (fork) =>
            fork.source === request.source &&
            Option.makeEquivalence(Equivalence.Number)(fork.offset, request.offset) &&
            fork.subOffset === request.subOffset,
    );
    // a header the request leaves out matches whatever the fork has
    const sameContentType = !Option.exists(
        request.contentType,
        (contentType) => mediaTypeOf(contentType) !== mediaTypeOf(stream.contentType),
    );
    const sameTtl = !Option.exists(
        request.ttlSeconds,
        (ttl) => !Option.contains(stream.ttlSeconds, ttl),
    );
    const sameExpiresAt = !Option.exists(
        request.expiresAt,
        (expiresAt) =>
            !Option.exists(stream.expiresAt, (stored) => DateTime.Equivalence(stored, expiresAt)),
    );
    return sameFork && sameContentType && sameTtl && sameExpiresAt;
};

/**
 * The source's checks for a fork request, in PROTOCOL.md 4.2's error table, and the point the fork
 * copies up to. JSON counts a sub-offset in messages: `offset + k`. Other types count bytes into the
 * message at `offset`, which the fork stores cut short as its last message. `messageBytes` is that
 * message's length, when there is one.
 */
export const resolveForkPoint = ({
    source,
    request,
    messageBytes,
}: {
    readonly source: StoredStream;
    readonly request: Pick<CreateForkRequest, 'offset' | 'subOffset' | 'contentType'>;
    readonly messageBytes: Option.Option<number>;
}): Effect.Effect<ForkPoint, ContentTypeMismatchError | InvalidForkOffsetError> =>
    Effect.gen(function* () {
        if (
            Option.exists(
                request.contentType,
                (contentType) => mediaTypeOf(contentType) !== mediaTypeOf(source.contentType),
            )
        ) {
            return yield* ContentTypeMismatchError.make({});
        }
        const offset = Option.getOrElse(request.offset, () => source.tail);
        if (offset > source.tail) {
            return yield* InvalidForkOffsetError.make({});
        }
        const point = {
            contentType: Option.getOrElse(request.contentType, () => source.contentType),
            ttlSeconds: source.ttlSeconds,
            expiresAt: source.expiresAt,
        };
        const k = request.subOffset;
        if (k === 0) {
            return { ...point, to: offset, lastBytes: Option.none() };
        }
        if (mediaTypeOf(source.contentType) === 'application/json') {
            if (offset + k > source.tail) {
                return yield* InvalidForkOffsetError.make({});
            }
            return { ...point, to: advance(offset, k), lastBytes: Option.none() };
        }
        // bytes into the message at `offset`, so there must be one, at least k bytes long
        if (!Option.exists(messageBytes, (bytes) => k <= bytes)) {
            return yield* InvalidForkOffsetError.make({});
        }
        return { ...point, to: advance(offset, 1), lastBytes: Option.some(k) };
    });

/**
 * PROTOCOL.md 4.2's table: a fork that sends its own TTL or Expires-At keeps it; one that sends
 * neither takes the source's.
 */
export const forkExpiry = ({
    point,
    request,
}: {
    readonly point: ForkPoint;
    readonly request: Pick<CreateForkRequest, 'ttlSeconds' | 'expiresAt'>;
}): Pick<StoredStream, 'ttlSeconds' | 'expiresAt'> =>
    Option.isSome(request.ttlSeconds) || Option.isSome(request.expiresAt)
        ? { ttlSeconds: request.ttlSeconds, expiresAt: request.expiresAt }
        : { ttlSeconds: point.ttlSeconds, expiresAt: point.expiresAt };
