# profiling — open work

Findings from the review of 2026-09-16, ordered by value. Evidence came from throwaway probes over the
`ws` fixture; none of this is covered by a test yet. Behaviour (1-3) first, cleanup (4-6), coverage (7-9).

## 1. The composite key search cannot find the usual business key

`_choose_composites` ranks candidate columns by descending cardinality and keeps `key_columns_max` (12).
A business key is normally one high-cardinality id plus one low-cardinality discriminator (leg, region,
currency, tenor) — exactly the columns that ranking drops.

Probe: 100k rows of `isin` (50k distinct, each twice) + `leg` (A/B) + 14 pseudo-random columns. The real
key is `isin+leg`; the report returned `isin+c0`, `isin+c1`, `isin+c10`, `isin+c11`, … and never
`isin+leg`. Reporting every surviving set does not help when the right set never enters the search.

Fix: pair the top-cardinality columns against *every* candidate column, prune with the necessary
condition `distinct(a) * distinct(b) >= rows`, and rank survivors by tightest distinct-product before
recurrence.

## 2. The committed report grows with the number of files

Measured on a 4-column table: ~510 bytes per file (file entry incl. csv sniff ~340 B, churn ~70 B per
candidate, volume ~40 B). 28 files -> 17 KB, 300 files -> ~155 KB, 750 files with 5 candidates ->
0.5-1 MB, rewritten whole on every run. Acceptable while 5 files were sampled; not since the default
became every file.

Fix: churn as first/last N plus an aggregate, sniff the first N files plus any whose hints differ, cap
`by_business_date`.

## 3. Null counts in the key tracker can be understated

A column with no non-null values in a batch never reaches `KeySet` (`TableProfile.add` only passes typed
columns), so those rows' nulls are not recorded. Probe: `code`, null for 600 of 2000 rows across 42
batches, recorded `nulls=58` — only the batch where nulls and values mixed. When the nulls align with
batch boundaries a nullable column is listed in `keys.candidate_columns`.

Fix: record every column's null count per batch in `KeyTracker`, or read nulls from the `ColumnProfile`.

## 4. Duplicated regexes

`LEADING_ZEROS` and `THOUSANDS` in `sample.py` are the same patterns as `leading_zeros` and
`thousands_separator` in `stats.QUIRKS`. They have to agree, or a column's `reason` contradicts its
`flags`. One definition, imported by both.

## 5. Loose typing

`model.Kind` is defined and used nowhere (`Typed.kind` is `str | None`): annotate with it or drop it.
The kind taxonomy is also split — `NUMERIC` / `TEMPORAL` / `KEYABLE` in `model.py`, `WIDEST` in
`propose.py`.

## 6. `rank = lambda ...  # noqa: E731`

In `KeyTracker._choose_composites`; the only suppressed lint in the module. Make it a `def`.

## 7. No test covers multi-batch merging

Every fixture CSV is one arrow batch, and arrow ignores a small `block_size` on a small file — about
2000 rows at `block_size=1024` are needed (gives ~42 batches). Since "statistics as values merged with
`+`" is the central design, one test should read a file in many batches and assert the merged lengths,
histogram, categorical counts, `varied` and per-file uniqueness.

## 8. Nested parquet columns changed behaviour silently

A struct or list column in a parquet file is now proposed as `data_type: json` (it used to be skipped and
never entered the committed schema). Verified by probe, untested and unmentioned in the README.

## 9. Runtime and second pass over the files

~40k rows/s for 16 columns (100k rows x 16 = 2.5 s; the composite search adds 0.35 s), so a full history
of 300 files x 500k rows is roughly an hour with no output until it finishes. Per-file progress would
help, and `--skim` should stay the documented loop while tuning a reader. `sniff_files` also walks the
whole file list a second time after sampling; it could fold into the sampling pass or stop after N files.
