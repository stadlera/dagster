---
name: onboard-tables
description: "Onboard one or more tables for an existing ingest Feed. Use when turning synced manifest paths into Patterns, delivery checks, readers, writers, committed schemas, loads, and provider scenario tests."
---

# Onboard tables

Outcome: an existing dataset package gains reviewed `Table` declarations, committed import schema and
profile reports, and a provider scenario test. The prerequisite is a validated feed and populated manifest;
use `onboard-feed` first when remote roots or sync behavior are still unknown.

Follow README.md "Adding a dataset", "Schema workflow", "Semantics worth knowing", and "Provider patterns".

## 1. Review the feed handoff

Review the `ingest.discover` JSON from `onboard-feed`, verify it against the manifest, and gather what is
still unknown:

- logical paths from at least three business dates for every filename family
- formats and dialects: delimiter, encoding, preamble, header, compression, and nesting
- cadence, business calendar, observed lag, and stable or variable files per business date
- retention moves, repacks, restatements, overwritten latest files, regions, and parts
- target table names and whether files are snapshots or incremental data

Do not infer a business calendar solely from a short mtime sample. Confirm it with the provider or owner.

## 2. Map file families to tables

- One table represents one target schema and load behavior, not necessarily one folder.
- Limit candidates with `Table(subsets=...)`; an empty tuple means every feed subset.
- Anchor patterns on logical paths: `^<subset>/...`.
- Capture the business date as `(?P<date>...)`; use other named groups only for values that must become
  columns or distinguish multiple logical files on one date.
- Test every regex against representative positives and near-miss negatives before syncing again.
- Declare `archives` only for containers of several business dates. A zip containing one day's entities
  remains one logical file.
- Begin with default identity and `on_collision="latest"`; change them only for a documented move,
  restatement, or repack behavior.

## 3. Declare delivery and loading

- Choose `Daily`, `Weekly`, `Monthly`, or `Yearly` partitioning from the business date semantics.
- Translate confirmed cadence into `Delivery`; use observed mtime lag as evidence for `lag_days`, with an
  operational allowance agreed by the owner.
- Use `.with_files(exactly=n)` only when historical counts are stable; otherwise use bounds or no exact count.
- Select and configure the reader from actual file evidence.
- Keep `ReplaceDay` for snapshots. Choose `Upsert(keys=...)` only after profiling and business-key review.

## 4. Classify, profile, and test

1. Add the smallest table declaration and a focused `Patterns.classify(path)` test.
2. Materialize `raw/<feed>` again. Check classified counts and ensure no `AmbiguousMatch` occurred.
3. Run `uv run python -m ingest.profile <feed>/<table> --skim` while tuning the reader, then a full profile.
4. Review report hints, flags, volume, candidate keys, and suggested merge behavior. Fix declarations and rerun.
5. Review and commit `schemas/import/<feed>.schema.yaml` and the profile report.
6. Load one partition; verify `_business_date`, `_subset`, pattern attributes, source traceability, and row counts.
7. Add `tests/test_dataset_<feed>.py`, following `tests/test_e2e.py`, with realistic names and small synthetic
   contents. Cover at least one provider quirk such as a move, restatement, repack, or second region.
8. Rerun the same partition and verify no duplicates, then run the repository validation commands.

## Completion checklist

- [ ] each pattern matches all examples for its family and rejects near misses and other tables
- [ ] every active wanted manifest row is classified exactly once or deliberately left unmatched
- [ ] delivery uses a confirmed calendar and a defensible lag/file-count policy
- [ ] reader settings agree with profile hints and the full profile completed
- [ ] schema YAML and profile report are committed
- [ ] a repeated load is idempotent and every row remains traceable to a manifest id
- [ ] scenario coverage captures at least one real provider behavior
- [ ] README "Provider patterns" is updated if the provider required a new pattern