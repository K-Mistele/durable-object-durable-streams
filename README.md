# Effect Durable Streams

A [Durable Streams](https://github.com/durable-streams/durable-streams) server and client built with Effect. One protocol engine runs on Cloudflare Workers + Durable Objects (deployed with alchemy) or on Effect Cluster.

## Packages

| Package                                        | Contents                                                                            |
| ---------------------------------------------- | ----------------------------------------------------------------------------------- |
| `@kylelayer/effect-durable-streams-protocol`   | Wire protocol: schemas, headers, offsets, errors, HttpApi and owner RPC definitions |
| `@kylelayer/effect-durable-streams-server`     | Protocol engine, storage ports, HttpApi handlers, in-memory host                    |
| `@kylelayer/effect-durable-streams-cloudflare` | Workers + Durable Objects host                                                      |
| `@kylelayer/effect-durable-streams-cluster`    | Effect Cluster host                                                                 |
| `@kylelayer/effect-durable-streams-client`     | Effect client                                                                       |

## Development

```bash
vp install          # install dependencies
bun run check       # format, lint, type check, typed lint
bun run test        # run every package's tests
bun run diff:check  # check changed lines and key files against HEAD (or pass --base)
bun run ready       # check + test
```
