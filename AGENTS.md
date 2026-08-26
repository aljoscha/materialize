# Materialize

## Engineering quality

Build the smallest coherent solution that fully solves the problem. Preserve
the requested capability and derive data from authoritative sources rather than
shipping snapshots or inert surfaces. Put responsibility in the subsystem that
already owns the behavior, with explicit contracts at storage, compute,
adapter, SQL, persist, catalog, and controller boundaries.

A change can be correct and still be wrong for the architecture. Review the
whole design for ownership, boundary clarity, duplicated concepts, failure and
recovery behavior, and whether a smaller complete shape fits the surrounding
system better. Findings name concrete code, contracts, or failure scenarios.

## Project knowledge and skills

Canonical agent skills live in `.agents/skills/`. `.claude/skills` is a
compatibility symlink. Check the applicable `mz-*` skill before work in that
area. The `mz-test` skill is authoritative for selecting and running tests,
including targeted checks during implementation.

For operation flow and crate ownership, consult:

- `doc/developer/generated/flows.md` for execution paths across the system.
- `doc/developer/generated/<crate>/_crate.md` for crate responsibilities.
- `doc/developer/generated/<crate>/<module>.md` for file-level structure.

`doc/developer/generated/` is owned exclusively by the recurring documentation
agent and its `update-docs` workflow. Other sessions treat the tree as
read-only, including when its contents are stale. Report problems rather than
editing, regenerating, staging, or committing generated documentation.

## Dependency contracts

Third-party versions belong in root `[workspace.dependencies]`. Member crates
use `dep.workspace = true`, with `optional = true` when needed. Do not inline a
version in a member manifest. Workspace features are the union needed by all
members.

Dependency changes preserve a focused `Cargo.lock` diff. Adding a dependency or
changing features may update the lock through `cargo check`. Update one package
with `cargo update -p <crate>` and `--precise` when needed. Never use bare
`cargo update`, and inspect any regenerated lockfile for unrelated version
changes.

License policy is defined jointly by `deny.toml` and `about.toml`. A newly
accepted SPDX license is added to both.
