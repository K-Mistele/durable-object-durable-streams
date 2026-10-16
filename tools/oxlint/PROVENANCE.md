# Local Oxlint rule provenance

- `tools/oxlint/anti-slop/` is an unmodified copy of
  `kyle-anti-slop/skills/install-anti-slop/assets/anti-slop` at `0548e24`.
  Do not edit it in place; re-sync with that skill's `install.mjs --force`.
- `automation/prefer-tagged-error-handling` was vendored from Fold's
  `tools/oxlint/automation` implementation. It reports `Predicate.isTagged`
  checks inside `Effect.catch`/`Effect.catchIf`; anti-slop rules keep
  ownership of direct `_tag` comparisons and switches.
