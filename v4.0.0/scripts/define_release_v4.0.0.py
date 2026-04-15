## Scripts to generate release artifacts for a new release
# Requires: metadata files from asap-crn-cloud-dataset-metadata
# .             for all datasets in the release of the correct CDE version

# steps:
# 1. DEFINE_RELEASE:
#          - Define tables which specify the dataset, and collection details for the current release
#          - usually builds of the previous release
#          - run from releases/v{version}/release_scripts/
# run `python3 define_release_{version}.py` first to generate datasets.csv, new_datasets.csv
# 2. COMPOSE RELEASE ARTIFACTS / ARCHIVE:
#          - run `python3 make_release_{generic}.py`
#          - run from releases/v{version}/release_scripts/
#          - Usage: python make_release_{generic}.py
#          - requires part 1 to be completed
#
# Below are unimplimented but represent the rest of the process
# 3. SYNC ARCHIVE:
#      - 3A. SYNC githubs
#      - 3A. SYNC dataset raw buckets
# unimplimented
# run sync_metadata_archive_{version}.py
# . TODO: make cleanup script to archive metadata back to asap-crn-cloud-dataset-metadata
# e.g. run sync_metadata_archive_{version}.py
# .  maybe also syncs to raw bucket ?

import pandas as pd
from pathlib import Path
import os, sys

import shutil
import json


# recommend:
# !pip3 install -e crn_utils_root
crn_utils_root = Path(__file__).resolve().parents[4] / "crn-utils"

# # Add crn-utils/src to the path so Python can find crn_utils as a package
# crn_utils_root = Path(__file__).resolve().parents[4] / "crn-utils" / "src"
# sys.path.insert(0, str(crn_utils_root))

from crn_utils.util import write_version, export_meta_tables, archive_CDE, load_tables
from crn_utils.release_util import (
    get_crn_release_metadata,
    get_stats_table,
    get_cohort_stats_table,
)
from crn_utils.constants import *
from crn_utils.bucket_util import gcloud_ls, gcloud_rsync

# resolve locations:
# Root of asap-crn-cloud-release-resources repo
release_resources_root = Path(__file__).resolve().parents[3]

# Root of asap-crn-cloud-dataset-metadata repo. Data is pulled from here
metadata_repo_root = (
    Path(__file__).resolve().parents[4] / "asap-crn-cloud-dataset-metadata"
)

# define last release
last_release = "v3.0.2"
release_type = "Urgent"

##################
# STEP 0:  Collect Previously platformed datasets
##################

ds_table = pd.read_csv(
    release_resources_root / "releases" / last_release / "datasets.csv"
)
ds_table.fillna("NA", inplace=True)
ds_tables = [ds_table.copy()]

##################
# STEP 1a:  Add new datasets compared to last release
##################
current_release = "v4.0.0"
current_cde = "v3.3"
release_type = "Major"

updated_ds_names = [
    "biederer-mouse-sc-rnaseq",
    "cragg-mouse-sn-rnaseq-striatum",
    "jakobsson-pmdbs-bulk-rnaseq",
    "scherzer-pmdbs-spatial-visium-mtg",
]

new_ds_names = [
    "sulzer-pmdbs-sn-rnaseq",
    "cohort-mouse-sc-rnaseq",  # removed by Javier becuase it's missing ~/asap-crn-cloud-dataset-metadata/datasets/cohort-mouse-sc-rnaseq/DOI/
]

ds_type_mapper = {
    "biederer-mouse-sc-rnaseq": "mouse-sc-rnaseq",
    "cragg-mouse-sn-rnaseq-striatum": "mouse-sc-rnaseq",
    "jakobsson-pmdbs-bulk-rnaseq": "pmdbs-bulk-rnaseq",
    "scherzer-pmdbs-spatial-visium-mtg": "pmdbs-spatial",
    "sulzer-pmdbs-sn-rnaseq": "pmdbs-sc-rnaseq",
    "cohort-mouse-sc-rnaseq": "mouse-sc-rnaseq",
}

ds_group_mapper = {
    "biederer-mouse-sc-rnaseq": "mouse-sc-rnaseq",
    "cragg-mouse-sn-rnaseq-striatum": "mouse-sc-rnaseq",
    "jakobsson-pmdbs-bulk-rnaseq": "pmdbs-bulk-rnaseq",
    "scherzer-pmdbs-spatial-visium-mtg": "pmdbs-spatial",
    "sulzer-pmdbs-sn-rnaseq": "pmdbs-sc-rnaseq",
    "cohort-mouse-sc-rnaseq": "mouse-sc-rnaseq",
}

### NOTE: here several values are hardcoded for new datasets

ds_table = pd.DataFrame()
## Lengths must match new_ds_names. Hence, because removed "cohort-mouse-sc-rnaseq" need to update here
## Original ['dataset_version'] = ["v1.0", "v1.0.1"]
## Original ["collection"] = ["pmdbs-sc-rnaseq", "mouse-sc-rnaseq"]
## Original ['dataset_version'] = [False,True]
ds_table["dataset_version"] = ["v1.0", "v1.0.0"]
ds_table["collection"] = ["pmdbs-sc-rnaseq", "mouse-sc-rnaseq"]
ds_table["cohort"] = [False, True]

ds_table["dataset_name"] = new_ds_names
ds_table["full_dataset_name"] = [
    f"team-{ds_table['dataset_name'][0]}",
    f"asap-{ds_table['dataset_name'][1]}",
]
ds_table["dataset_type"] = ds_table["dataset_name"].map(ds_type_mapper)
ds_table["team_name"] = ds_table["full_dataset_name"].apply(
    lambda x: "-".join(x.split("-")[:2])
)
ds_table["team"] = ds_table["team_name"].apply(lambda x: x.split("-")[-1])
ds_table["workflow"] = ds_table["collection"].str.replace("-", "_")
ds_table["cde_version"] = current_cde
ds_table["grouping"] = ds_table["dataset_name"].map(ds_group_mapper)
ds_table["latest_release"] = current_release
ds_table["release_type"] = "Major"

# placeholders for now
ds_table["collection_name"] = "NA"
ds_table["collection_version"] = "NA"

# set bucket names here note "prod" bucket is named -> "curated"
ds_table["raw_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-raw-{x.lstrip('asap-')}"
)
ds_table["dev_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-dev-{x.lstrip('asap-')}"
)
ds_table["uat_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-uat-{x.lstrip('asap-')}"
)
ds_table["prod_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-curated-{x.lstrip('asap-')}"
)

# need to add collection versions.
collection_versions = {
    "pmdbs-sc-rnaseq": "v3.1.0",
    "pmdbs-bulk-rnaseq": "v1.2.0",
    "pmdbs-spatial": "v1.1.0",
    "mouse-spatial": "v1.0.0",
    "mouse-sc-rnaseq": "v1.0.0",
    "NA": "NA",
}

ds_table["collection_version"] = ds_table["collection"].map(collection_versions)

##############
ds_tables.append(ds_table)
datasets_table = pd.concat(ds_tables).reset_index(drop=True)


##############
datasets_table["collection_version"] = datasets_table["collection"].map(
    collection_versions
)

########
# force collection and collection name and collection version for new datasets
datasets_table.loc[datasets_table["cohort"] == True, "dataset_version"] = (
    datasets_table.loc[datasets_table["cohort"] == True, "collection_version"]
)
datasets_table.loc[
    datasets_table["dataset_name"] == "jakobsson-pmdbs-bulk-rnaseq",
    "collection",
] = "pmdbs-bulk-rnaseq"

datasets_table.loc[
    datasets_table["dataset_name"] == "biederer-mouse-sc-rnaseq",
    "collection",
] = "mouse-sc-rnaseq"

datasets_table.loc[
    datasets_table["dataset_name"] == "cragg-mouse-sn-rnaseq-striatum",
    "collection",
] = "mouse-sc-rnaseq"

datasets_table.loc[
    datasets_table["dataset_name"] == "sulzer-pmdbs-sn-rnaseq",
    "collection",
] = "pmdbs-sc-rnaseq"

datasets_table.loc[
    datasets_table["dataset_name"] == "scherzer-pmdbs-spatial-visium-mtg",
    "collection",
] = "pmdbs-spatial"

# force the workflow
datasets_table["workflow"] = datasets_table["collection"].str.replace("-", "_")

# fix spatial
datasets_table.loc[
    datasets_table["dataset_name"] == "scherzer-pmdbs-spatial-visium-mtg",
    "workflow",
] = "spatial_visium"

# fix spatial
datasets_table.loc[
    datasets_table["dataset_name"] == "cragg-mouse-spatial-visium-striatum",
    "workflow",
] = "spatial_visium"

# fix spatial
datasets_table.loc[
    datasets_table["dataset_name"] == "team-edwards-pmdbs-spatial-geomx-th",
    "workflow",
] = "spatial_geomx"


###################
### MAJOR RELEASE UPDATES
###################
release_version = "v4.0.0"
collections = [
    "pmdbs-sc-rnaseq",
    "pmdbs-bulk-rnaseq",
    "pmdbs-spatial",
    "mouse-spatial",
    "mouse-sc-rnaseq",
]

grouping_mapper = {
    "pmdbs-sc-rnaseq": "pmdbs-sc-rnaseq",
    "pmdbs-bulk-rnaseq": "pmdbs-bulk-rnaseq",
    "pmdbs-other": "other-pmdbs",
    "mouse-sc-rnaseq": "mouse-sc-rnaseq",
    "pmdbs-spatial": "pmdbs-spatial",
    "mouse-spatial": "mouse-spatial",
    "invitro-bulk-rnaseq": "invitro",  # update? # NOTE: ask Andy--Why this? #ANS: organization on VWB
    "proteomics": "proteomics",
}

collection_name = {
    "pmdbs-sc-rnaseq": "PMDBS scRNAseq",
    "pmdbs-bulk-rnaseq": "PMDBS bulkRNAseq",
    "pmdbs-spatial": "PMDBS Spatial RNAseq",
    "mouse-spatial": "Mouse Spatial RNAseq",
    "mouse-sc-rnaseq": "Mouse scRNAseq",
}

# NOTE: ask Andy--Why this exception?
# . answer schlossmacher was NOT curated
# exception
# "schlossmacher-mouse-sn-rnaseq-osn-aav-transd": "other-mouse",
datasets_table.loc[
    datasets_table["dataset_name"] == "schlossmacher-mouse-sn-rnaseq-osn-aav-transd",
    "grouping",
] = "other-mouse"

datasets_table.loc[
    datasets_table["dataset_name"] == "alessi-mouse-sn-rnaseq-dorsal-striatum-g2019s",
    "grouping",
] = "other-mouse"


# set all cde to v3.3
datasets_table["cde_version"] = current_cde
datasets_table["latest_release"] = current_release

# remap collections.
datasets_table["collection_version"] = datasets_table["collection"].map(
    collection_versions
)
datasets_table["collection_name"] = datasets_table["collection"].map(collection_name)
datasets_table["release_type"] = "Major"

###################
### STEP 1b: Saving datasets.csv for new release
###################
print(f"\n\nSTEP 1b: Saving datasets.csv for new release\n\n")
current_release = "v4.0.0"
release_path = release_resources_root / "releases" / current_release
if not release_path.exists():
    release_path.mkdir(parents=True)

# for a major release new_datasets and datasets are identical
datasets_table.to_csv(release_path / "new_datasets.csv", index=False)
datasets_table.to_csv(release_path / "datasets.csv", index=False)


# below is a stub for exporting release metadata to the metadata archive
if False:
    for row, dataset in datasets_table.iterrows():
        dataset_name = dataset["dataset_name"]
        dataset_in_metadata_repo = metadata_repo_datasets / dataset_name
        source_mdata_path = (
            dataset_in_metadata_repo / "metadata" / "release" / current_release
        )
        dataset_version = dataset["dataset_version"]
        schema_version = dataset["cde_version"]

        # Export metadata to metadata/release/latest_release/
        print(f"Exporting {dataset_name} to {source_mdata_path}")
        if not source_mdata_path.exists():
            source_mdata_path.mkdir(parents=True)
        export_meta_tables(dfs, source_mdata_path)
        write_version(schema_version, source_mdata_path / "cde_version")
        write_version(dataset_version, source_mdata_path / "dataset_version")
