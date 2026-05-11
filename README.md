# ASAP CRN Cloud Releases

Source-of-truth archive for ASAP CRN versioned releases. Each release is a DOI-backed, immutable snapshot that binds a specific set of datasets and collections together under a single version.

This repository is automatically managed by the [cloud-orchestration](https://github.com/ASAP-CRN/cloud-orchestration) system. Manual changes should be avoided.

## Current Releases

9 releases as of v4.0.0: `v1.0.0`, `v2.0.0`, `v2.0.1`, `v2.0.2`, `v2.0.3`, `v3.0.0`, `v3.0.1`, `v3.0.2`, `v4.0.0`

| Release | Datasets | New | Collections | CDE Version |
|---------|----------|-----|-------------|-------------|
| v1.0.0  | 5        | 5   | 1           | v2.1        |
| v2.0.0  | 11       | 6   | 2           | v3.0        |
| v2.0.1  | 12       | 1   | 2           | v3.0        |
| v2.0.2  | 15       | 3   | 2           | v3.1        |
| v2.0.3  | 16       | 1   | 2           | v3.2        |
| v3.0.0  | 16       | 0   | 4           | v3.2        |
| v3.0.1  | 21       | 5   | 4           | v3.3        |
| v3.0.2  | 23       | 2   | 4           | v3.2        |
| v4.0.0  | 25       | 2   | 5           | v3.3        |

## Structure

```
releases.json                          # Master index: per-release dataset/collection snapshots
releases/
└── releases.json                      # Mirror of root releases.json (symlink or copy)
<release-version>/                     # One directory per release version
├── release.json                       # Full release manifest (see schema below)
├── scripts/                           # Release-specific scripts (may be depricated)
└── *.pdf                              # Release README PDF
```

### File Roles

- **`releases.json`** (root) — compact index used by tooling; keyed by release version. Each entry contains `all_datasets`, `new_datasets`, and `all_collections` with full dataset/collection metadata at the time of that release.
- **`releases/releases.json`** — identical copy of the root `releases.json`, provided for tooling that expects it under the `releases/` subdirectory.
- **`<version>/release.json`** — authoritative manifest for a single release; includes the release DOI, CDE version, creation timestamp, and lists of datasets (`datasets`, `new_datasets`) and collections (`collections`) with their DOIs and versions.

## Release Schema

### `<version>/release.json`

example:
```json
{
  "release_version": "v4.0.1",
  "cde_version": "v3.3",
  "release_doi": "10.5281/zenodo.17834620",
  "datasets": [
    {
      "name": "hafler-pmdbs-sn-rnaseq-pfc",
      "doi": "10.5281/zenodo.15490150",
      "version": "v1.0"
    },
    {
      "name": "sulzer-pmdbs-sn-rnaseq",
      "doi": "10.5281/zenodo.17612853",
      "version": "v1.0"
    }
  ],
  "new_datasets": [
    {
      "name": "sulzer-pmdbs-sn-rnaseq",
      "doi": "10.5281/zenodo.17612853",
      "version": "v1.0"
    }
  ],
  "collections": [
    {
      "name": "pmdbs-sc-rnaseq",
      "doi": "10.5281/zenodo.16979638",
      "version": "v3.1.0"
    }
  ],
  "created": "2026-04-07T14:50:24Z",
  "metadata": {
    "total_datasets": 25,
    "total_collections": 5
  }
}
```

- **`datasets`**: All datasets included in this release (name, DOI, version)
- **`new_datasets`**: Datasets added or updated for the first time in this release
- **`collections`**: All versioned collections included in this release (name, DOI, version)
- **`cde_version`**: The Common Data Elements schema version applied across all datasets in this release
- **`release_doi`**: Zenodo concept DOI for this release

### `releases.json` (index)

```json
{
  "v4.0.0": {
    "all_datasets": [ { "name": "...", "doi": "...", "version": "..." } ],
    "new_datasets":  [ { "name": "...", "doi": "...", "version": "..." } ],
    "all_collections": [ { "name": "...", "doi": "...", "version": "..." } ]
  }
}
```

Each key is a release version. Fields mirror `release.json` but use `all_datasets` / `all_collections` to distinguish them from release-specific subsets.

## Versioning Scheme

Release versions follow `vMAJOR.MINOR.PATCH`:

- **Major** — new tissue type, modality, or substantial scope change
- **Minor** — new datasets added to an existing collection, or curation workflow updates that regenerate curated data
- **Patch** — metadata corrections, DOI updates, or minor fixes

Each release version also tracks:
- **CDE Version** — which Common Data Elements schema was applied
- **Dataset Version** — per-dataset version at time of release
- **Collection Version** — per-collection version at time of release

## Release Process

1. Datasets and collections are prepared and assigned Dataset DOIs via Zenodo
2. A release version tag is created in cloud-orchestration
3. `<version>/release.json` is generated with the full dataset/collection manifest
4. `releases.json` index is updated with the new release entry
5. All associated repositories (`cloud-datasets`, `cloud-collections`) are updated
6. Zenodo records are published and DOIs are finalized
