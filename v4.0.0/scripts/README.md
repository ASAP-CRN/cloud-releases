## Scripts in releases/v4.0.0/release_scripts/

The scripts here are examples of what is required to generate a release.

Requires: metadata files from asap-crn-cloud-dataset-metadata for all datasets in the release of the correct CDE version

steps to release:
1. DEFINE_RELEASE: `define_release_4.0.0.py`
         - Define tables which specify the dataset, and collection details for the current release
         - usually builds of the previous release
         - run from releases/v{version}/release_scripts/
         - run `python3 define_release_{version}.py` first to generate datasets.csv, new_datasets.csv
        - NEXT STEPS:  functionalize / refactor, cli

2. COMPOSE RELEASE ARTIFACTS / ARCHIVE: `make_release_generic.py`
         - run `python3 make_release_{generic}.py`
         - run from releases/v{version}/release_scripts/
         - Usage: python make_release_{generic}.py
         - requires part 1 to be completed
         - NEXT STEPS:  functionalize and make cli

3. SYNC ARCHIVE: (not fully demonstrated here)
     - 3A. SYNC githubs
     - 3A. SYNC dataset metadata and file_metadata to raw buckets
         - NEXT STEPS:  define process and make functions, cli


## files:
- `define_release_4.0.0.py` : defines the datasets.csv and new_datasets.csv files
- `make_release_generic.py` : generates the release artifacts
- `transfer_file_metadata_v4.0.0.py` : transfers the file_metadata to the raw buckets