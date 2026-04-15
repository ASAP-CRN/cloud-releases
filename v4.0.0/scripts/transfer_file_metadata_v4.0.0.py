## Script to sync each datasets file_metadata to raw buckets
# run AFTERR make_release_generic.py


import pandas as pd
from pathlib import Path
import os, sys

import shutil
import json


# recommend:
# !pip3 install -e crn_utils_root
# crn_utils_root = Path(__file__).resolve().parents[4] / "crn-utils"

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

###################
### STEP 1: load datasets.csv for new release
###################
current_release = "v4.0.0"
release_path = release_resources_root / "releases" / current_release
datasets_table = pd.read_csv(release_path / "datasets.csv")


###################
### STEP 2: loope over all datasets and copy the file_metadata to the raw bucket
###################
# copy release_path / "datasets" / dataset_name / "file_metadata/curated_files.csv" to raw bucket...
for row, dataset in datasets_table.iterrows():
    dataset_name = dataset["dataset_name"]
    release_ds_path = release_path / "datasets" / dataset_name
    file_metadata_path = release_ds_path / "file_metadata"
    print(
        f"copying {dataset_name}/file_metadata to {dataset['raw_bucket_name']}/file_metadata"
    )

    raw_bucket_name = dataset["raw_bucket_name"]

    destination = f"{raw_bucket_name}/file_metadata/"
    gcloud_rsync(file_metadata_path, destination, directory=True)
    print(f"Uploaded {file_metadata_path} to {destination}")


###################
### STEP X: example loop for sybncing non-file metadata
###################
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

###################
# below is a a fragment for exporting release metadata to the resource bucket

# %%
# sync the release folder to the release bucket

# platform_bucket = "gs:/asap-crn-cloud-release-resources"


# # %%
# gsutil -m rsync -dr "releases/v4.0.0/" "gs://asap-crn-cloud-release-resources/releases/v4.0.0/"

# gsutil -m cp -r "gs://asap-crn-cloud-release-resources/releases/v4.0.0/datasets/*"  "gs://asap-crn-cloud-release-resources/"
# gsutil -m cp -r "releases/v4.0.0/collections" "gs://asap-crn-cloud-release-resources/"


# ## warning these clobber things...
# # gsutil -m rsync -dr "gs://asap-crn-cloud-release-resources/releases/v3.0.0/datasets/" "gs://asap-crn-cloud-release-resources/"
# # gsutil -m rsync -dr "gs://asap-crn-cloud-release-resources/releases/v3.0.0/collections/" "gs://asap-crn-cloud-release-resources/collections/"

# echo "v4.0.0" | gsutil cp - gs://asap-crn-cloud-release-resources/release_version
# gsutil -m cp "resource/*.csv" "gs://asap-crn-cloud-release-resources/CDE/"
