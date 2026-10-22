import { Array as Arr, Effect, Schema, pipe } from 'effect';
import { Base64, Sse } from 'effect/encoding';
import { Cursor, mediaTypeOf, Offset } from './codec';

// PROTOCOL.md 5.8: cursor is required unless the stream is closed
export const SseControl = Schema.Struct({
    streamNextOffset: Offset,
    streamCursor: Schema.optionalKey(Cursor),
    upToDate: Schema.optionalKey(Schema.Boolean),
    streamClosed: Schema.optionalKey(Schema.Boolean),
}).check(
    Schema.makeFilter(
        (control) =>
            control.streamCursor !== undefined ||
            control.streamClosed === true ||
            'streamCursor is required unless streamClosed',
    ),
);
export type SseControl = typeof SseControl.Type;

// text/* and application/json go as text; anything else is base64
export const isBase64 = (contentType: string): boolean => {
    const mediaType = mediaTypeOf(contentType);
    return !(mediaType.startsWith('text/') || mediaType === 'application/json');
};

export const sseDataEvent = (data: string) =>
    Sse.Event.make({ event: 'data', id: undefined, data });

export const sseBinaryDataEvent = (bytes: Uint8Array) => sseDataEvent(Base64.encode(bytes));

// also the keep-alive: sent when a wait times out with no new data
export const sseControlEvent = (control: SseControl) =>
    Schema.encodeEffect(Schema.fromJsonString(SseControl))(control).pipe(
        Effect.map((data) => Sse.Event.make({ event: 'control', id: undefined, data })),
    );

// Upstream writes "data:" with no space, and the conformance suite matches it literally. A parser
// drops one space after the colon, so a line that starts with a space gets one more.
export const renderSseEvent = (event: Sse.Event): string => {
    const dataLines = pipe(
        event.data.split(/\r\n|\r|\n/),
        Arr.map((line) => (line.startsWith(' ') ? `data: ${line}\n` : `data:${line}\n`)),
        Arr.join(''),
    );
    return `event: ${event.event}\n${dataLines}\n`;
};
