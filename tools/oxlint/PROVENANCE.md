# Local Oxlint rule provenance

- `anti-slop/no-conditional-empty-array-spread` was vendored from Synclayer's
  `tools/oxlint/anti-slop` implementation.
- `anti-slop-effect/prefer-tagged-error-handling` was vendored from Fold's
  `tools/oxlint/automation` implementation.
- `anti-slop/no-reprovide-ambient-service` was vendored from Ampere's
  `tools/oxlint/anti-slop` implementation.

Both are enabled at error severity. To avoid duplicate diagnostics, the Fold
rule has a targeted boundary: it reports `Predicate.isTagged` checks inside
`Effect.catch`/`Effect.catchIf`. Existing anti-slop rules retain ownership of
direct `_tag` comparisons and switches inside and outside broad Effect catches.
