import { Offset } from './codec';

export const zero = Offset.make(0);

// returns the offset after appending `messages` messages
export const advance = (offset: Offset, messages: number): Offset => Offset.make(offset + messages);
