---
name: onboard-feed
description: "Discover and onboard a provider remote as an ingest Feed. Use when inspecting SFTP or fsspec file inventories, inferring subsets, depth, exclusions, sync cron, or creating the initial dataset package before tables exist."
---

# Onboard a feed

Outcome: `src/ingest/datasets/<name>/__init__.py` exposes a `dataset` with a validated `Feed` and
`tables=()`, and one raw sync confirms the remote paths without downloading known unwanted files.
Stop at the populated manifest; use `onboard-tables` next.

Follow README.md "Stages and layout", "Adding a dataset", and "Provider patterns". Treat heuristics as
evidence to review, never as permission to guess provider semantics.

## 1. Establish remote access

Collect the facts that cannot be inferred:

- feed name and a provider-owned root from which discovery may list recursively
- fsspec protocol, host, port, username, and password or key authentication
- secret environment variable names; declare secrets as `EnvVar("<FEED>_...")`
- permitted listing scope and whether filenames or mtimes are sensitive

Do not download file contents during discovery. Do not put secret values in commands, reports, tests,
or committed files.

## 2. Inventory and infer

List paths with file type, size, and mtime. Keep enough history to distinguish a repeated delivery from
one coincidence. Group the evidence by immediate folder, filename shape, extension, and extracted date.
Use the metadata-only discovery CLI when the provider root is known:

```bash
uv run python -m ingest.discover /out --protocol sftp \
  --option host=sftp.acme.com --option port=22 --option username=us \
  --option-env password=ACME_PASSWORD
```

Use `--maxdepth` to bound an initially broad listing. Secret options must use `--option-env`; never place
their values in arguments. Review the JSON output rather than copying it directly into declarations.

Propose, with examples and confidence or caveats:

- **subsets**: candidate provider roots that hold distinct data; retention/archive folders remain inside
  a subset unless they are independently delivered roots
- **maxdepth**: deepest wanted relative file plus one, matching `fsspec.find` semantics
- **exclude**: basename regexes only for files that should never be landed, such as an overwritten static
  latest copy; temporary files may instead need a table-level ignore after mirroring
- **cron**: a run time after the observed delivery mtime, with a buffer; report timezone assumptions
- dated filename regexes and `date_format` as handoff evidence for `onboard-tables`
- observed cadence, mtime-to-business-date lag, and files per date as delivery evidence, not declarations

Use at least three dated occurrences when suggesting cadence or lag. Flag timezone-naive or missing mtimes,
mixed date formats, shallow history, and multimodal arrival times instead of collapsing them into one answer.

## 3. Compare evidence to `Feed`

Account for every field explicitly:

| `Feed` field | Source |
|---|---|
| `name` | user/provider naming decision; never inferred from a folder alone |
| `remote` | supplied connection and auth facts; never inferred from files |
| `cron` | buffered observed arrival mtimes, then operator confirmation |
| `subsets` | reviewed folder/root candidates |
| `maxdepth` | maximum wanted depth within each chosen root; one feed value must cover all subsets |
| `exclude` | reviewed basename-only regex; leave `None` when uncertain |

Call out evidence that cannot fit one feed. Different connections or incompatible sync windows usually mean
different feeds; differing table cadence does not.

## 4. Declare and verify

1. Create the dataset package with `feed` and `Dataset(feed, tables=(), schema_dir=...)`.
2. Add focused tests for subset-name validation and any non-obvious exclusion regex.
3. Run `uv run dagster definitions validate`.
4. Materialize `raw/<feed>` once against the development remote.
5. Inspect `subset`, `path`, `remote_mtime`, `size`, and `status` in the manifest. Confirm every chosen root
   lands under `<feed>/<subset>/`, exclusions are `ignored`, and no wanted file was omitted by `maxdepth`.
6. Preserve the discovery JSON with the dated path examples, cadence evidence, and provider quirks as the
  handoff to `onboard-tables`. Do not commit it when paths or metadata are sensitive.

## Completion checklist

- [ ] connection details are parameterized and no secret value is committed
- [ ] every subset is a simple folder name and maps to an existing provider root
- [ ] `maxdepth` reaches all wanted files without broad accidental scope
- [ ] `exclude` is anchored and tested against wanted and unwanted basenames
- [ ] cron timezone and mtime timezone are explicit; the schedule includes an operational buffer
- [ ] definitions validate and a second raw sync downloads nothing new
- [ ] manifest evidence is ready for `onboard-tables`