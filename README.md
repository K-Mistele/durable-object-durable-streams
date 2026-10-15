# Vite+ Monorepo Starter

A starter for creating a Vite+ monorepo.

## Development

- Run formatting, Oxlint/type checks, and the TypeOnce typed checks:

```bash
bun run check
```

- Check only changed lines and key files against `HEAD` (or pass `--base`):

```bash
bun run diff:check
```

- Check everything is ready:

```bash
vp run ready
```

- Run the tests:

```bash
vp run -r test
```

- Build the monorepo:

```bash
vp run -r build
```

- Run the development server:

```bash
vp run dev
```
