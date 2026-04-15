
# %% 
# #### generate a table defining the datasets for a release 
#  dataset_name, bucket_name, collection_name
#
#

# %%
import pandas as pd
from pathlib import Path
import os, sys
import json

import shutil

from crn_utils.util import get_dataset_version, write_version, get_release_version, get_cde_version, export_meta_tables, archive_CDE, load_tables
from crn_utils.release_util import create_metadata_package, get_crn_release_metadata, get_stats_table, get_cohort_stats_table
from crn_utils.constants import *

%load_ext autoreload
%autoreload 2

root_path = Path.cwd().parent 

## 
# NOTE:  these "Urgent" releases could simply copy the previous release artifacts, and add the new ones...
# %%
# TODO:  these should be read from the github repos in the future..
##################
# STEP 0:  Collect Previously platformed datasets
##################
# ALL DATASETS

last_release = "v2.0.0"

release_path_base = root_path / "releases" / last_release

# datasets_table = pd.read_csv(release_path_base / "datasets.csv")

ds_table = pd.read_csv(release_path_base / "datasets.csv")

ds_table.fillna("NA", inplace=True)

ds_tables = [ds_table]

# %%
## DEFINITIONS:   
#  now add v2.0.1 release

last_release = "v2.0.1"

release_path_base = root_path / "releases" / last_release


###################
### STEP -1: make CDE refs, Collection Refs, Dataset Refs via github
###################
# TODO: these should be read from the github repos in the future..

cde_version = "v3.1"
dataset_version = "v2.0"  # all the same for now

###################
### STEP 0.A: define datasets by collections, collection names and datasets
###################
release_version = "v2.0.1"
collections = ["pmdbs-sc-rnaseq", "pmdbs-bulk-rnaseq", "pmdbs-other"]

collection_name = {
    "pmdbs-sc-rnaseq" : "PMDBS scRNAseq",
    "pmdbs-bulk-rnaseq" : "PMDBS bulkRNAseq",
    # "pmdbs-other" : "Other PMDBS Data"
}


collection_versions = {
    "pmdbs-sc-rnaseq" : "v2.0.0",
    "pmdbs-bulk-rnaseq" : "v1.0.0",
    "NA":"NA"
    # "pmdbs-other" : "v1.0.0"
}

# %%
###########
# define datasets
datasets = pd.DataFrame()

#1 "" 
dataset_type = "pmdbs-sc-rnaseq"
dataset_names = [
        "jakobsson-pmdbs-sn-rnaseq",
        ]

datasets['dataset_name'] = dataset_names
datasets['full_dataset_name'] = "team-" + datasets['dataset_name']
datasets['dataset_type'] = dataset_type



# %%

# %%
datasets['dataset_version'] = dataset_version
datasets['team_name'] = datasets["full_dataset_name"].apply(lambda x: "-".join(x.split("-")[:2]))
datasets['team'] = datasets["team_name"].apply(lambda x: x.split("-")[-1])


# %%
datasets["collection"] = "NA" #datasets["dataset_type"]
datasets["collection_name"] = "NA" #datasets["collection"].map(collection_name)
datasets["workflow"] = "NA" 
# datasets["collection_version"] = datasets["collection"].map(collection_versions)
# datasets["workflow"] = datasets["dataset_type"].map({
#     "pmdbs-sc-rnaseq" : "pmdbs_sc_rnaseq",
#     "pmdbs-bulk-rnaseq" : "pmdbs_bulk_rnaseq"})

# %%

# set bucket names here note "prod" bucket is named -> "curated"
datasets["raw_bucket_name"] = datasets["dataset_name"].apply(
    lambda x: f"gs://asap-raw-team-{x}"
)
datasets["dev_bucket_name"] = datasets["dataset_name"].apply(
    lambda x: f"gs://asap-dev-team-{x}"
)
datasets["uat_bucket_name"] = datasets["dataset_name"].apply(
    lambda x: f"gs://asap-uat-team-{x}"
)
datasets["prod_bucket_name"] = datasets["dataset_name"].apply(
    lambda x: f"gs://asap-curated-team-{x}"
)

# fix cohort names
datasets["raw_bucket_name"] = datasets["raw_bucket_name"].apply(
    lambda x: x.replace("team-cohort-", "cohort-")
)
datasets["dev_bucket_name"] = datasets["dev_bucket_name"].apply(
    lambda x: x.replace("team-cohort-", "cohort-")
)
datasets["uat_bucket_name"] = datasets["uat_bucket_name"].apply(
    lambda x: x.replace("team-cohort-", "cohort-")
)
datasets["prod_bucket_name"] = datasets["prod_bucket_name"].apply(
    lambda x: x.replace("team-cohort-", "cohort-")
)



# %%
datasets["cde_version"] = cde_version
datasets["grouping"] = "pmdbs-other"
datasets.loc[datasets["dataset_type"]=="pmdbs-other","collection"] = "NA"
datasets["cohort"] = datasets["dataset_name"].apply(lambda x: "cohort" in x)
datasets["latest_release"] = release_version
datasets["release_type"] = "Urgent"

# %%


release_path = root_path / "releases" / release_version 


## export datasets as "new_datasets.csv" and "datasets.csv". This is accurate because all of the datasets got updated curation
datasets.to_csv(release_path / "new_datasets.csv", index=False)

ds_tables.append(datasets)
# %%

datasets_table = pd.concat(ds_tables).reset_index(drop=True)

# %%


datasets_table.to_csv(release_path / "datasets.csv", index=False)

# %%
# %%
### TODO:  add code to copy the metadata for all "new" datasets from github 
#.  NOTE:  for a Major release ALL the datasets are "new"

metadata_root = root_path.parent / "asap-crn-cloud-dataset-metadata"
datasets_path = metadata_root / "datasets"
map_path = metadata_root / "asap-ids/master"

last_release = "v2.0.1"
release_path_base = root_path / "releases" / last_release
datasets_table = pd.read_csv(release_path_base / "datasets.csv")

# issue... we have two jakobsson versions... we need to force the v2 jakobsson into the
#    jakobsson-pmdbs-sn-rnaseq-v2 folder

for row, dataset in datasets_table.iterrows():

    # for dataset in datasets["dataset_name"]:
    dataset_name = dataset["dataset_name"]
    ds_path = datasets_path / dataset_name
    

    release_ds_path = release_path / "datasets" / dataset_name

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]

    latest_release = dataset["latest_release"]

    if "jakobsson" in dataset_name and dataset_version == "v2.0":
        release_ds_path =  release_path / "datasets" / "jakobsson-pmdbs-sn-rnaseq-v2"


    # type
    # source = 
    ds_source = dataset["dataset_type"]
    # cohort
    cohort = dataset["cohort"]

    spatial = "spatial" in dataset_name
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = PMDBS_TABLES
        table_names = table_names + ["SPATIAL"] if spatial else table_names
    elif ("mouse" in ds_source):
        source = "mouse"
        table_names = MOUSE_TABLES
        table_names = table_names + ["SPATIAL"] if spatial else table_names
    elif ("cell" in ds_source):
        source = "cell"
        table_names = CELL_TABLES
    else:
        source = ds_source


    if cohort:
        # get cohort stats 
        # only PMDBS at this point
        print(f"warning, cohort stats only defined for PMDBS") 

        source_metadata_path = ds_path / "metadata/release" / latest_release
        dest_metadata_path = release_ds_path / "metadata"
        shutil.copytree(source_metadata_path, dest_metadata_path, dirs_exist_ok=True)

        source_file_metadata_path = ds_path / "file_metadata" / latest_release
        dest_file_metadata_path = release_ds_path / "file_metadata"
        shutil.copytree(source_file_metadata_path, dest_file_metadata_path, dirs_exist_ok=True)
        
        # copy DOI
        package_source = ds_path / "DOI"
        package_destination = release_ds_path / "DOI"
        shutil.copytree(package_source, package_destination, dirs_exist_ok=True)

        # write version
        write_version(dataset_version, release_ds_path/"version")

        dfs = load_tables(source_metadata_path, table_names)
        report,df = get_cohort_stats_table(dfs, source)
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

    else:

        suffix = "ids"
        dfs = get_crn_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)

    
        final_metadata_path = release_ds_path / "metadata"
        if not final_metadata_path.exists():
            final_metadata_path.mkdir(parents=True)

        export_meta_tables(dfs, final_metadata_path)
        write_version(schema_version, final_metadata_path / "cde_version")


        write_version(dataset_version, release_ds_path/"version")

        # copy DOI
        package_source = ds_path / "DOI"
        package_destination = release_ds_path / "DOI"

        package_destination.mkdir(exist_ok=True)

        dataset_doi = package_source / "dataset.doi"
        readme_path = package_source / f"{dataset_name}_README.pdf"

        shutil.copy2(dataset_doi, package_destination / "dataset.doi")
        shutil.copy2(readme_path, package_destination / f"{dataset_name}_README.pdf")


        # copy file_metadata
        file_metadata_path = ds_path / "file_metadata"
        # check that the folder is not empty
        if file_metadata_path.is_dir():
            # check that the folder is not empty
            if not list(file_metadata_path.iterdir()):
                print(f"Skipping empty folder {file_metadata_path}")
                continue
            else:
                dest = release_ds_path / file_metadata_path.name
                # dest.mkdir(exist_ok=True)
                shutil.copytree(file_metadata_path, dest, dirs_exist_ok=True)

                print(f"Copied {file_metadata_path} to {dest}")



        report,df = get_stats_table(dfs, source)

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






#%% 
#.  create Collection summary 

# get non-empty collections
collections = datasets_table["collection"].dropna().unique()

for collection in collections:
    datasets = datasets_table.loc[datasets_table["collection"]==collection, "dataset_name"].values.tolist()
    print(f"Collection: {collection} has datasets: {datasets}")
    # confirm all have the same collection_version
    collection_table = datasets_table[datasets_table["collection"]==collection]
    collection_version = collection_table["collection_version"].unique()
    assert len(collection_version) == 1, f"Multiple collection versions for {collection}: {collection_version}"
    collection_version = collection_version[0]
    collection_path = release_path / "collections" / collection
    if not collection_path.exists():
        collection_path.mkdir(parents=True)
    write_version(collection_version, collection_path/"collection_version")
    collection_table.to_csv(collection_path / "datasets.csv", index=False)

    # copy DOI from f"cohort-{collection}"
    # if collection_table["workflow"] is not nan
    if not collection_table["workflow"].isna().values[0]:
        package_source = release_path / "datasets" / f"cohort-{collection}" / "DOI"
        package_destination = collection_path / "DOI"
        shutil.copytree(package_source, package_destination, dirs_exist_ok=True)





#%% 
#.  archive CDE 


resource_path = root_path / "resource"

for schema in datasets_table["cde_version"].unique():
    archive_CDE(schema, resource_path)





# %%
