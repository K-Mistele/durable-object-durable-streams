/**
 * The Worker: the HttpApi over `layerDurableObject`, one `StreamObject` per stream path.
 */
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Http from 'alchemy/Http';
import { Effect, Layer } from 'effect';
import * as HttpRouter from 'effect/http/HttpRouter';
import { layerDurableStreamsHttpApi } from '@kylelayer/effect-durable-streams-server/http-api';
import { layerDurableObject } from './streams-durable-object';

// Http.Platform: HttpPlatform, Etag, Path and a no-op FileSystem, for runtimes without files
const layerApp = layerDurableStreamsHttpApi.pipe(
    Layer.provide([layerDurableObject, Http.Platform]),
);

export default class DurableStreamsWorker extends Cloudflare.Worker<DurableStreamsWorker>()(
    'DurableStreamsWorker',
    { main: import.meta.url },
    Effect.gen(function* () {
        return { fetch: yield* HttpRouter.toHttpEffect(layerApp) };
    }),
) {}
