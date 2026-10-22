import { Context, type Effect, type Stream } from 'effect';
import type { StreamPath } from '@kylelayer/effect-durable-streams-protocol/codec';
import type { StreamNotFoundError } from '@kylelayer/effect-durable-streams-protocol/errors';
import type {
    AppendToStreamError,
    CreateStreamError,
    MissingStreamError,
} from './stream-operations';
import type {
    AppendRequest,
    AppendResult,
    CreateStreamRequest,
    CreateStreamResult,
    ReadBodyRequest,
    ReadRange,
    StartReadRequest,
    StreamMetadata,
    WaitForNewMessagesRequest,
    WaitResult,
} from './stream-operation-requests';

type StreamTarget = {
    readonly stream: StreamPath;
};

/**
 * What the HTTP handlers call: each `StreamOperations` operation with the stream path added.
 * Each host provides the layer that reaches the stream's `StreamOperations`.
 */
export class Streams extends Context.Service<
    Streams,
    {
        readonly createStream: (
            request: CreateStreamRequest & StreamTarget,
        ) => Effect.Effect<CreateStreamResult, CreateStreamError>;
        readonly appendToStream: (
            request: AppendRequest & StreamTarget,
        ) => Effect.Effect<AppendResult, AppendToStreamError>;
        readonly getMetadata: (
            request: StreamTarget,
        ) => Effect.Effect<StreamMetadata, MissingStreamError>;
        readonly startRead: (
            request: StartReadRequest & StreamTarget,
        ) => Effect.Effect<ReadRange, MissingStreamError>;
        readonly readBody: (
            request: ReadBodyRequest & StreamTarget,
        ) => Stream.Stream<Uint8Array, StreamNotFoundError>;
        readonly waitForNewMessages: (
            request: WaitForNewMessagesRequest & StreamTarget,
        ) => Effect.Effect<WaitResult, MissingStreamError>;
        readonly deleteStream: (request: StreamTarget) => Effect.Effect<void, MissingStreamError>;
    }
>()('@kylelayer/effect-durable-streams-server/streams') {}
