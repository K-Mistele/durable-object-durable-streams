/**
 * The protocol's rules for creating and appending, with no I/O. A rule that can refuse a request
 * returns an Effect that fails with the protocol error; the rest return plain values.
 * `StreamOperations` runs them inside the storage transaction with one `now` per request, so the
 * expiry check and the timestamps it writes agree.
 */
import { Data, DateTime, Effect, Equivalence, Match, Option } from 'effect';
import {
    mediaTypeOf,
    type ProducerSeq,
    type StreamSeq,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    ContentTypeMismatchError,
    InvalidEpochSeqError,
    SequenceGapError,
    StaleEpochError,
    type StreamClosedError,
    StreamConfigConflictError,
    StreamNotFoundError,
    StreamSeqConflictError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import { advance, zero } from '@kylelayer/effect-durable-streams-protocol/offset';
import {
    decideProducer,
    type ProducerState,
} from '@kylelayer/effect-durable-streams-protocol/producer';
import type { ProducerRequestId, StoredStream } from './stored-stream';
import type { AppendRequest } from './stream-operation-requests';

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
    | StreamClosedError
    | ContentTypeMismatchError
    | StaleEpochError
    | InvalidEpochSeqError
    | SequenceGapError
    | StreamSeqConflictError;

export const isExpired = ({
    stream,
    now,
}: {
    readonly stream: StoredStream;
    readonly now: DateTime.Utc;
}): boolean => {
    const pastExpiresAt = Option.exists(stream.expiresAt, (expiresAt) =>
        DateTime.isGreaterThanOrEqualTo(now, expiresAt),
    );
    const pastTtl = Option.exists(stream.ttlSeconds, (seconds) =>
        DateTime.isGreaterThanOrEqualTo(now, DateTime.add(stream.lastAccessedAt, { seconds })),
    );
    return pastExpiresAt || pastTtl;
};

/** Fails with 404 when nothing is stored at the path or the stored stream has expired: an expired stream no longer exists. */
export const getExistingStream = ({
    stored,
    now,
}: {
    readonly stored: Option.Option<StoredStream>;
    readonly now: DateTime.Utc;
}): Effect.Effect<StoredStream, StreamNotFoundError> => {
    if (Option.isNone(stored) || isExpired({ stream: stored.value, now })) {
        return Effect.fail(StreamNotFoundError.make({}));
    }
    return Effect.succeed(stored.value);
};

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

/** PROTOCOL.md 5.1: a PUT on an existing stream with a different config gets 409. */
export const rejectConfigConflict = ({
    existing,
    requested,
}: {
    readonly existing: StoredStream;
    readonly requested: StreamConfig;
}): Effect.Effect<void, StreamConfigConflictError> => {
    if (!hasSameConfig({ stream: existing, config: requested })) {
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
        Option.exists(stream.lastStreamSeq, (lastSeq) => seq <= lastSeq),
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
