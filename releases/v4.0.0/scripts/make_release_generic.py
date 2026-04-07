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

from crn_utils.util import (
    write_version,
    export_meta_tables,
    archive_CDE,
    load_tables,
    read_meta_table,
)
from crn_utils.release_util import (
    get_crn_release_metadata,
    get_stats_table,
    get_cohort_stats_table,
)
from crn_utils.constants import *
from crn_utils.bucket_util import gcloud_ls, gcloud_rsync

%load_ext autoreload
%autoreload 2

# %%
# #################
# Steps
# STEP 0. define paths and release
### STEP 1: Load datasets.csv
### STEP 2: Getting tables, export/copy metadata, copy DOIs, generate stats for each dataset
### STEP 3: file_metadata
### STEP 4: collections + cohort dataset metadata compilation, stats. (DOI by hand)
###      - if Major Release - construct
###      - if Minor/Urgent Release - copy cohort dataset info from previous release
###      - collections/ only defined for Major/Minor releases
### STEP 5: Archiving CDE
# #################

# ####################
# STEP 0. define paths and release variables
# ####################
# resolve locations:
# Root of asap-crn-cloud-release-resources repo
release_resources_root = Path(__file__).resolve().parents[3]

# Root of asap-crn-cloud-dataset-metadata repo. Data is pulled from here
metadata_repo_root = (
    Path(__file__).resolve().parents[4] / "asap-crn-cloud-dataset-metadata"
)


# edit the release version and cde version in the following cell
current_release = "v4.0.0"
previous_release = "v3.0.2"
release_type = "Major"

release_path = release_resources_root / "releases" / current_release

###################
### STEP 1:Load datasets.csv
###################

datasets_table = pd.read_csv(release_path / "datasets.csv")

metadata_repo_datasets = metadata_repo_root / "datasets"
map_path = metadata_repo_root / "asap-ids/master"

###################
### STEP 2: Getting tables, export/copy metadata, copy DOIs, generate stats for each dataset (skip cohort)
###################
print(
    f"\n\nSTEP 2: Getting tables, export/copy metadata, copy DOIs, generate stats for each non-cohort dataset\n\n"
)
for row, dataset in datasets_table.iterrows():
    print(f"Processing {dataset['dataset_name']}")


    dataset_name = dataset["dataset_name"]
    dataset_in_metadata_repo = metadata_repo_datasets / dataset_name
    release_ds_path = release_path / "datasets" / dataset_name

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]
    latest_release = dataset["latest_release"]

    ds_source = dataset["dataset_type"]
    cohort = dataset["cohort"]
    proteomics = False
    spatial = "spatial" in dataset_name

    if cohort:
        print(f"Skipping {dataset['dataset_name']} as cohort")
        continue

    #####################
    ### Get list of tables based on source
    ### NOTE: TABLES added here are defined in crn-utils/src/crn_utils/constants.py
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = PMDBS_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")
            print("added spatial")

    elif "mouse" in ds_source:
        source = "mouse"
        table_names = MOUSE_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")

    elif "invitro" in ds_source:
        source = "invitro"
        table_names = CELL_TABLES.copy()

    elif "proteomics" in ds_source:
        source = "invitro"
        proteomics = True
        table_names = PROTEOMICS_TABLES.copy()

    else:
        source = ds_source

    #####################
    ### Metadata is copied from latest release (for cohorts) or exported (for non-cohorts)
    ### DOI are copied
    ### Stats are generated and saved as JSON and CSV

    suffix = "ids"
    dfs = get_crn_release_metadata(
        dataset_in_metadata_repo,
        schema_version,
        map_path,
        suffix,
        spatial=spatial,
        proteomics=proteomics,
        source=source,
    )

    ## NOTE: these steps export identical metadata and DOIs to two locations
    # Export metadata to metadata/
    final_metadata_path = release_ds_path / "metadata"
    print(f"Exporting {dataset_name} to {final_metadata_path}")
    if not final_metadata_path.exists():
        final_metadata_path.mkdir(parents=True)
    export_meta_tables(dfs, final_metadata_path)
    write_version(schema_version, final_metadata_path / "cde_version")
    write_version(dataset_version, release_ds_path / "version")

    # copy DOI
    package_source = dataset_in_metadata_repo / "DOI"
    package_destination = release_ds_path / "DOI"
    package_destination.mkdir(exist_ok=True)
    dataset_doi = package_source / "dataset.doi"
    readme_path = package_source / f"{dataset_name}_README.pdf"
    shutil.copy2(dataset_doi, package_destination / "dataset.doi")
    shutil.copy2(readme_path, package_destination / f"{dataset_name}_README.pdf")

    # generate stats for the dataset
    # note PROTEOMICS are mapped to "cell" through "invitro"
    report, df = get_stats_table(dfs, source)
    # collect stats
    if report:
        # write json report
        filename = release_ds_path / "release_stats.json"
        with open(filename, "w") as f:
            json.dump(report, f, indent=4)

        # write stats table
        df.to_csv(release_ds_path / "release_stats.csv", index=False)

    else:
        print(f"Skipping stats for {dataset_name}")

# %%
###################
### STEP 3: File metadata
###################
# define helper function
# TODO: move to crn_utils
def gen_dev_bucket_summary(
    dev_bucket_name: str,
    workflow_name: str,
    dl_path: Path,
    dataset_name: str,
    flatten: bool = False,
) -> list[str]:

    bucket_path = (
        f"{dev_bucket_name.split('/')[-1]}"  # dev_bucket_name has gs:// prefix
    )
    print(f"Processing {bucket_path}")
    ## OTHER and everything else...
    # create a list of the curated files in /cohort_analysis
    if workflow_name == "NA":
        print(f"Skipping {dataset_name} as workflow is NA")
        return []
    elif workflow_name in [
        "pmdbs_bulk_rnaseq",
        "pmdbs_sc_rnaseq",
        "spatial_geomx",
        "spatial_visium",
        "mouse_sc_rnaseq",
    ]:  # "pmdbs_bulk_rnaseq":
        prefix = f"{workflow_name}/**"
        # also downstream + upstream
    else:
        print(f"Skipping {dataset_name} as workflow {workflow_name} is not implemented")
        return []

    project = None
    artifacts = gcloud_ls(bucket_path, prefix, project=project)
    # drop empty strings, files that start with ".", and folders
    artifact_files = [
        f for f in artifacts if f != "" and Path(f).name[0] != "." and f[-1] != "/"
    ]
    return artifact_files


###################
# do it in parts to deal with messed up MANIFEST.tsv
# for dataset_name, curated_files in all_curated_files.items():

print(f"\n\nSTEP 3: Getting file_metadata\n\n")
for row, dataset in datasets_table.iterrows():

    # if not dataset["cohort"]:
    #     print(f"Skipping {dataset['dataset_name']} as not cohort")
    #     continue

    # for dataset in datasets["dataset_name"]:
    dataset_name = dataset["dataset_name"]
    ds_path = metadata_repo_datasets / dataset_name

    release_ds_path = release_path / "datasets" / dataset_name

    # first copy whatever we have...
    dataset_in_metadata_repo = metadata_repo_datasets / dataset_name
    # copy file_metadata
    file_metadata_path = dataset_in_metadata_repo / "file_metadata"
    if file_metadata_path.is_dir():
        # skipping empty folders
        if not list(file_metadata_path.iterdir()):
            print(f"Skipping empty folder {file_metadata_path}")
            continue
        else:
            dest = release_ds_path / file_metadata_path.name

            # don't copy subfolders
            if not dest.exists():
                dest.mkdir(parents=True)
            n = 0
            for f in file_metadata_path.iterdir():
                if f.is_file():
                    shutil.copy2(f, dest)
                    n += 1
                # ERROR: for some reason this does not copy the accurate raw_files.csv for the cohort-pmdbs-bulk-rnaseq dataset
                # copying by hand.
            # shutil.copytree(file_metadata_path, dest, dirs_exist_ok=True)
            print(f"Copied {n} files from {file_metadata_path} to {dest}")

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]
    latest_release = dataset["latest_release"]

    dataset = dataset.fillna("NA")
    workflow = dataset["workflow"]

    print(f"Processing {dataset_name}")
    dev_bucket_name = dataset["dev_bucket_name"]
    dl_path = release_ds_path / "file_metadata"
    curated_files = gen_dev_bucket_summary(
        dev_bucket_name, workflow, dl_path, dataset_name
    )
    # curated_files = all_curated_files[dataset_name]
    if len(curated_files) == 0:
        print(f"Skipping {dataset_name} as no curated files")
        continue

    print(f"Dataset: {dataset_name} has {len(curated_files)} curated files")
    curated_files_df = pd.DataFrame(curated_files, columns=["curated_files"])

    # artifacts.csv
    # ASAP_dataset_id,ASAP_team_id,artifact_type,file_name,timestamp,workflow,workflow_version,gcp_uri,bucket_md5

    # raw_files.csv
    # ASAP_dataset_id,ASAP_team_id,ASAP_sample_id,file_name,replicate,batch,file_MD5,file_type,gcp_uri,sample_name,bucket_md5

    curated_files_df["filename"] = curated_files_df["curated_files"].apply(
        lambda x: x.split("/")[-1]
    )
    curated_files_df["file_path"] = curated_files_df["curated_files"].apply(
        lambda x: "/".join(x.split("/")[3:-1])
    )

    curated_files_df["bucket_name"] = curated_files_df["curated_files"].apply(
        lambda x: x.split("/")[3]
    )
    curated_files_df["workflow"] = curated_files_df["file_path"].apply(
        lambda x: x.split("/")[0]
    )

    curated_files_df["artifact_type"] = curated_files_df["file_path"].apply(
        lambda x: "/".join(x.split("/")[1:])
    )
    curated_files_df["archive"] = curated_files_df["artifact_type"].apply(
        lambda x: x.startswith("archive")
    )
    # drop all "archive" rows
    curated_files_df = curated_files_df[~curated_files_df["archive"]]

    # get "curated_files" for all filenames == "MANIFEST.tsv"
    manifest_files = curated_files_df[curated_files_df["filename"] == "MANIFEST.tsv"]

    # download manifests
    manifests_df = pd.DataFrame()
    for index, row in manifest_files.iterrows():
        remote = row["curated_files"]
        local = (
            release_path
            / "datasets"
            / dataset_name
            / "file_metadata"
            / f"{row["file_path"].replace('/', '-')}-{row['filename']}"
        )
        gcloud_rsync(remote, local, directory=False)
        print(f"Downloaded {remote} to {local}")
        df = pd.read_csv(local, sep="\t")

        # HACK:  drop columns with missing workflow_version
        df = df.dropna(subset=["workflow_version"], how="all")
        # they don't always have the same columns... so outer join instead
        # manifests_df = manifests_df.merge(df, on="filename", how="outer")

        manifests_df = pd.concat([manifests_df, df])
        # WARNING: PosixPath('/Users/ergonyc/Projects/ASAP/asap-crn-cloud-release-resources/releases/v3.0.0/datasets/scherzer-pmdbs-sn-rnaseq-mtg/file_metadata/pmdbs_sc_rnaseq-cohort_analysis-MANIFEST.tsv')
        #  has an error... fixing by hand...

    # rename manifests_df workflow to workflow_y
    manifests_df = manifests_df.rename(columns={"workflow": "workflow_y"})
    # merge manifests_dfs into curated_files_df
    curated_files_df = curated_files_df.merge(manifests_df, on="filename", how="left")

    # assert workflow = workflow_y where workflow_y is not null
    assert (
        curated_files_df.loc[curated_files_df["workflow_y"].notna(), "workflow"]
        == curated_files_df.loc[curated_files_df["workflow_y"].notna(), "workflow_y"]
    ).all(), "workflow and workflow_y do not match"

    # add ASAP_dataset_id and ASAP_team_id to curated_files_df
    # get it by reading the metadata/STUDY.csv
    # encode ASAP_dataset_id and ASAP_team_id as "COHORT" for cohort datasets

    # important change!
    # TODO: retroactively do this for v2.0.0 and v3.0.0
    if dataset["cohort"]:
        ASAP_dataset_id = (
            f"COHORT_{dataset["collection"]}_{dataset["collection_version"]}"
        )
        ASAP_team_id = f"COHORT_{dataset["collection"]}"
    else:
        study_file = release_path / "datasets" / dataset_name / "metadata/STUDY.csv"
        study_df = pd.read_csv(study_file)
        ASAP_dataset_id = study_df["ASAP_dataset_id"].unique()[0]
        ASAP_team_id = study_df["ASAP_team_id"].unique()[0]

    input_cols = [
        "ASAP_dataset_id",
        "ASAP_team_id",
        "artifact_type",
        "filename",
        "file_path",
        "timestamp",
        "workflow",
        "workflow_version",
        "gcp_uri",
    ]
    curated_files_df["gcp_uri"] = (
        dataset["prod_bucket_name"] + "/" + curated_files_df["file_path"]
    )
    curated_files_df["ASAP_dataset_id"] = ASAP_dataset_id
    curated_files_df["ASAP_team_id"] = ASAP_team_id
    curated_files_df = curated_files_df[input_cols]

    # save curated_files_df to file_metadata/curated_files.csv
    curated_files_df.to_csv(
        release_path / "datasets" / dataset_name / "file_metadata/curated_files.csv",
        index=False,
    )


###################
### STEP 4: Creating cohort / Creating collection summaries
###################
#####################
### DOI are copied by hand
### Stats are generated and saved as JSON and CSV

print(f"\n\nSTEP 4: Creating collection summaries\n\n")
collections = datasets_table["collection"].dropna().unique()
for collection in collections:
    collection_datasets_table = datasets_table[
        datasets_table["collection"] == collection
    ]

    datasets = collection_datasets_table["dataset_name"].values.tolist()

    print(f"Collection: {collection} has datasets: {datasets}")
    # confirm all have the same collection_version
    collection_table = datasets_table[datasets_table["collection"] == collection]
    collection_version = collection_table["collection_version"].unique()
    assert (
        len(collection_version) == 1
    ), f"Multiple collection versions for {collection}: {collection_version}"
    collection_version = collection_version[0]
    collection_path = release_path / "collections" / collection
    if not collection_path.exists():
        collection_path.mkdir(parents=True)
    write_version(collection_version, collection_path / "collection_version")
    collection_table.to_csv(collection_path / "datasets.csv", index=False)

    # doing this by hand for now...
    # # copy DOI from f"cohort-{collection}"
    # # if collection_table["workflow"] is not nan
    # if not collection_table["workflow"].isna().values[0]:
    #     package_source = release_path / "datasets" / f"cohort-{collection}" / "DOI"
    #     package_destination = collection_path / "DOI"
    #     if package_source.exists():
    #         shutil.copytree(package_source, package_destination, dirs_exist_ok=True)
    #     else:
    #         print(
    #             f"Skipping DOI copy for {collection} as {package_source} does not exist"
    #         )
    if "rnaseq" in collection:

        combined_dfs = {}
        combined_raw_dfs = pd.DataFrame()
        for row, dataset in collection_table.iterrows():

            print(f"Processing {dataset['dataset_name']}")
            dataset_name = dataset["dataset_name"]
            dataset_in_metadata_repo = metadata_repo_datasets / dataset_name
            release_ds_path = release_path / "datasets" / dataset_name

            dataset_version = dataset["dataset_version"]
            schema_version = dataset["cde_version"]
            latest_release = dataset["latest_release"]

            ds_source = dataset["dataset_type"]
            cohort = dataset["cohort"]

            proteomics = False
            spatial = "spatial" in dataset_name

            tables = MOUSE_TABLES.copy() if "mouse" in ds_source else PMDBS_TABLES.copy()
            source = "mouse" if "mouse" in ds_source else "pmdbs"

            ds_path = release_path / "datasets" / dataset_name

            ds_release_path = ds_path / "metadata"

            if not cohort:  # only gather raw files for non-cohorts
                # ds_release_path = ds_path / "metadata" / release
                dfs = load_tables(ds_release_path, tables)
                raw_df = read_meta_table(ds_path / "file_metadata/raw_files.csv")

                # copy metadata from latest release

                if combined_dfs == {}:  # first time through
                    combined_dfs = dfs
                    combined_raw_dfs = raw_df
                    print(f"first time through for {collection}")
                else:
                    for tab in tables:
                        if tab not in dfs:
                            continue
                        combined_dfs[tab] = pd.concat(
                            [combined_dfs[tab], dfs[tab]], ignore_index=True
                        )

                    combined_raw_dfs = pd.concat(
                        [combined_raw_dfs, raw_df], ignore_index=True
                    )
            else:
                # we should already have the other file_metadata/curated_files.csv
                # define the path to the cohort dataset release path
                cohort_dataset_path = release_path / "datasets" / dataset_name
                cohort_metadata_path = cohort_dataset_path / "metadata"
                cohort_file_metadata_path = cohort_dataset_path / "file_metadata"

        print(f"finished all datasets for {collection}")

        # write combined metadata
        if not cohort_metadata_path.exists():
            cohort_metadata_path.mkdir(parents=True, exist_ok=True)
        export_meta_tables(combined_dfs, cohort_metadata_path)
        # export_meta_tables(dfs, metadata_path)
        write_version(schema_version, cohort_metadata_path / "cde_version")

        if not cohort_file_metadata_path.exists():
            cohort_file_metadata_path.mkdir(parents=True, exist_ok=True)
        combined_raw_dfs.to_csv(cohort_file_metadata_path / "raw_files.csv", index=False)

        # write collection version
        write_version(collection_version, cohort_metadata_path / "collection_version")
        write_version(collection_version, cohort_file_metadata_path / "collection_version")
        write_version(collection_version, cohort_dataset_path / "version")

        report, df = get_cohort_stats_table(combined_dfs, source)
        # collect stats
        if report:
            # write json report
            filename = cohort_dataset_path / "release_stats.json"
            with open(filename, "w") as f:
                json.dump(report, f, indent=4)
            # write stats table
            df.to_csv(cohort_dataset_path / "release_stats.csv", index=False)
        else:
            print(f"Skipping stats for {cohort_dataset_path}")


###################
### STEP 5: Archiving CDE
###################
print(f"\n\nSTEP 5: Archiving CDE\n\n")
for schema in datasets_table["cde_version"].unique():
    archive_CDE(schema, release_resources_root / "resource")


###################
### STEP 6: regenerate stats. (not needed but harmless to regenerate)
###################
print(f"\n\nSTEP 6: generate stats for each dataset\n\n")
for row, dataset in datasets_table.iterrows():
    
    print(f"Processing {dataset['dataset_name']}")
    dataset_name = dataset["dataset_name"]


    dataset_in_metadata_repo = metadata_repo_datasets / dataset_name
    release_ds_path = release_path / "datasets" / dataset_name

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]
    latest_release = dataset["latest_release"]

    ds_source = dataset["dataset_type"]
    cohort = dataset["cohort"]
    proteomics = False
    spatial = "spatial" in dataset_name

    #####################
    ### Get list of tables based on source
    ### NOTE: TABLES added here are defined in crn-utils/src/crn_utils/constants.py
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = PMDBS_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")
            print("added spatial")

    elif "mouse" in ds_source:
        source = "mouse"
        table_names = MOUSE_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")

    elif "invitro" in ds_source:
        source = "invitro"
        table_names = CELL_TABLES.copy()

    elif "proteomics" in ds_source:
        source = "invitro"
        proteomics = True
        table_names = PROTEOMICS_TABLES.copy()

    else:
        source = ds_source

    #####################
    ### Metadata is copied from latest release (for cohorts) or exported (for non-cohorts)
    ### DOI are copied
    ### Stats are generated and saved as JSON and CSV

    dfs = load_tables(release_ds_path / "metadata", table_names)
    # generate stats for the dataset
    # note PROTEOMICS are mapped to "cell" through "invitro"

    if not cohort:
        report, df = get_stats_table(dfs, source)
    else:
        report, df = get_cohort_stats_table(dfs, source)

    # collect stats
    if report:
        # write json report
        filename = release_ds_path / "release_stats.json"
        with open(filename, "w") as f:
            json.dump(report, f, indent=4)

        # write stats table
        df.to_csv(release_ds_path / "release_stats.csv", index=False)

    else:
        print(f"Skipping stats for {dataset_name}")


# %%
# updated GCP URIs in
# collect curated_files.csv from all releases and combine
#
# . - metadata/DATA.csv
# - file_metadata/curated_files.csv
# - file_metadata/artifact_files.csv
# - file_metadata/raw_files.csv

# from crn_utils.bucket_util import (
#     authenticate_with_service_account,
#     gsutil_ls,
#     gsutil_cp,
#     gsutil_ls2,
#     gsutil_cp2,
# )

# from crn_utils.checksums import extract_md5_from_details2, get_md5_hashes
