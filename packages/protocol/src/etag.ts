import { Base64 } from 'effect/encoding';
import type { Offset } from './codec';

// PROTOCOL.md:602, :1351. The id is base64 because stream paths can contain ':' and '"';
// ':c' makes the ETag change when the stream closes, so a 304 can't hide the close
export const etagFor = (stream: {
    readonly id: string;
    readonly start: '-1' | Offset;
    readonly end: Offset;
    readonly closed: boolean;
}): string =>
    `"${Base64.encode(stream.id)}:${stream.start}:${stream.end}${stream.closed ? ':c' : ''}"`;
