import { Effect, Schema } from 'effect';
import { Base64, Sse } from 'effect/encoding';
import { joinBytes } from './bytes';
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

/** How a data event carries a stream's bytes: base64 for binary, as they are for text, as one array for JSON. */
export type SseDataEncoding = 'base64' | 'text' | 'json';

export const sseDataEncodingFor = (contentType: string): SseDataEncoding => {
    if (isBase64(contentType)) {
        return 'base64';
    }
    return mediaTypeOf(contentType) === 'application/json' ? 'json' : 'text';
};

/**
 * A data event written in pieces, so a large message goes out part by part instead of whole.
 * Together the pieces are what effect's `Sse.encoder` writes for the whole payload: one `data:`
 * line, each CR, LF or CRLF in the payload starting a new one. Base64 stays on one line, since
 * the base64 of whole 3-byte groups joins into the base64 of the whole.
 */
export type SseDataEvent = {
    readonly encoding: SseDataEncoding;
    /** base64: the last 0–2 bytes, not yet encoded */
    readonly unencoded: Uint8Array;
    /** text and JSON: the last byte written was CR, so a first byte LF finishes a CRLF */
    readonly endsWithCarriageReturn: boolean;
    /** JSON: no message written yet, so the next one needs no comma */
    readonly hasNoMessages: boolean;
};

const utf8 = new TextEncoder();
const carriageReturn = 13;
const lineFeed = 10;
const nextDataLine = utf8.encode('\ndata: ');

export const openSseDataEvent = (
    encoding: SseDataEncoding,
): readonly [SseDataEvent, Uint8Array] => [
    {
        encoding,
        unencoded: new Uint8Array(0),
        endsWithCarriageReturn: false,
        hasNoMessages: true,
    },
    utf8.encode(encoding === 'json' ? 'event: data\ndata: [' : 'event: data\ndata: '),
];

/**
 * Before a message's first bytes. JSON separates messages with a comma, which ends any CRLF; text
 * messages join directly, so a CR ending one and an LF starting the next are one line break.
 */
export const startSseDataMessage = (event: SseDataEvent): readonly [SseDataEvent, Uint8Array] => {
    if (event.encoding !== 'json' || event.hasNoMessages) {
        return [{ ...event, hasNoMessages: false }, new Uint8Array(0)];
    }
    return [{ ...event, endsWithCarriageReturn: false }, utf8.encode(',')];
};

export const writeSseData = (
    event: SseDataEvent,
    bytes: Uint8Array,
): readonly [SseDataEvent, Uint8Array] => {
    if (event.encoding === 'base64') {
        const pending = joinBytes([event.unencoded, bytes]);
        const wholeGroups = pending.byteLength - (pending.byteLength % 3);
        return [
            { ...event, unencoded: pending.slice(wholeGroups) },
            utf8.encode(Base64.encode(pending.subarray(0, wholeGroups))),
        ];
    }
    // text and JSON: each CR, LF or CRLF starts the next line with `data: `
    const pieces: Array<Uint8Array> = [];
    let lineStart = 0;
    let afterCarriageReturn = event.endsWithCarriageReturn;
    bytes.forEach((byte, index) => {
        if (byte === carriageReturn) {
            pieces.push(bytes.subarray(lineStart, index), nextDataLine);
            lineStart = index + 1;
            afterCarriageReturn = true;
        } else if (byte === lineFeed) {
            pieces.push(bytes.subarray(lineStart, index));
            if (!afterCarriageReturn) {
                pieces.push(nextDataLine);
            }
            lineStart = index + 1;
            afterCarriageReturn = false;
        } else {
            afterCarriageReturn = false;
        }
    });
    pieces.push(bytes.subarray(lineStart));
    return [{ ...event, endsWithCarriageReturn: afterCarriageReturn }, joinBytes(pieces)];
};

export const closeSseDataEvent = (event: SseDataEvent): Uint8Array => {
    if (event.encoding === 'base64') {
        return utf8.encode(`${Base64.encode(event.unencoded)}\n\n`);
    }
    return utf8.encode(event.encoding === 'json' ? ']\n\n' : '\n\n');
};

// also the keep-alive: sent when a wait times out with no new data
export const sseControlEvent = (control: SseControl) =>
    Schema.encodeEffect(Schema.fromJsonString(SseControl))(control).pipe(
        Effect.map((data) =>
            utf8.encode(
                Sse.encoder.write(Sse.Event.make({ event: 'control', id: undefined, data })),
            ),
        ),
    );
