import { Array as Arr } from 'effect';

/** One `Uint8Array` holding `pieces` in order. */
export const joinBytes = (pieces: ReadonlyArray<Uint8Array>): Uint8Array => {
    const joined = new Uint8Array(
        Arr.reduce(pieces, 0, (total, piece) => total + piece.byteLength),
    );
    Arr.reduce(pieces, 0, (at, piece) => {
        joined.set(piece, at);
        return at + piece.byteLength;
    });
    return joined;
};
