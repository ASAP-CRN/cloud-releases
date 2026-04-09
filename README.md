# ASAP CRN Cloud Releases

Source-of-truth archive for ASAP CRN versioned releases. Each release is a DOI-backed, immutable snapshot that binds a specific set of datasets and collections together under a single version.

This repository is automatically managed by the [cloud-orchestration](https://github.com/ASAP-CRN/cloud-orchestration) system. Manual changes should be avoided.

## Current Releases

9 releases as of v4.0.0: `v1.0.0`, `v2.0.0`, `v2.0.1`, `v2.0.2`, `v2.0.3`, `v3.0.0`, `v3.0.1`, `v3.0.2`, `v4.0.0`

## Structure

```
releases.json                          # Master index of all releases
releases/<release-version>/
├── release.json                       # Full release snapshot (see schema below)
└── scripts/                           # Release-specific scripts
```

`releases.json` is the top-level index; each entry links to the corresponding `release.json`.

## Release Schema

```json
{
  "release_version": "v4.0.0",
  "cde_version": "v3.3",
  "release_doi": "10.5281/zenodo.xxxxxxx",
  "datasets": [
    {
      "name": "hafler-pmdbs-sn-rnaseq-pfc",
      "doi": "10.5281/zenodo.xxxxxxx",
      "version": "v1.0"
    }
  ],
  "new_datasets": [
    {
      "name": "hafler-pmdbs-sn-rnaseq-pfc",
      "doi": "10.5281/zenodo.xxxxxxx",
      "version": "v1.0"
    }
  ],
  "collections": [
    {
      "name": "pmdbs-sc-rnaseq",
      "doi": "10.5281/zenodo.xxxxxxx",
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

- **`datasets`**: All datasets included in this release
- **`new_datasets`**: Datasets added or updated for the first time in this release
- **`collections`**: All versioned collections included in this release
- **`cde_version`**: The Common Data Elements version applied across datasets in this release

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

1. Datasets and collections are prepared and assigned DOIs via Zenodo
2. A release version tag is created in cloud-orchestration
3. `release.json` is generated with the full dataset/collection manifest
4. All associated repositories (`cloud-datasets`, `cloud-collections`) are updated
5. Zenodo records are published and DOIs are finalized
