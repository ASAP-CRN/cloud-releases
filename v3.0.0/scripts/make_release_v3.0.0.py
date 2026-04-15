
# %% 
# #### generate a table defining the datasets for a release 
#  dataset_name, bucket_name, collection_name
#
#

# %%
import pandas as pd
from pathlib import Path
import os, sys

import shutil
import json

from crn_utils.util import get_dataset_version, write_version, get_release_version, get_cde_version, export_meta_tables, archive_CDE, load_tables
from crn_utils.release_util import create_metadata_package, get_crn_release_metadata, get_stats_table, get_cohort_stats_table
from crn_utils.constants import *
from crn_utils.bucket_util import authenticate_with_service_account, gsutil_ls, gsutil_cp, gsutil_ls2, gsutil_cp2

%load_ext autoreload
%autoreload 2

root_path = Path.cwd().parent 


# %%
# TODO:  these should be read from the github repos in the future..
##################
# STEP 0:  Collect Previously platformed datasets
##################
# ALL DATASETS

last_release = "v2.0.3"

release_path_base = root_path / "releases" / last_release

datasets_table = pd.read_csv(release_path_base / "datasets.csv")

ds_table = pd.read_csv(release_path_base / "datasets.csv")
ds_table.fillna("NA", inplace=True)

ds_tables = [ds_table]


# %%
##################
# STEP 1a:  Add new datasets ("Urgent")
##################
current_release = "v3.0.0"
current_cde = "v3.2"

# col_names = ['dataset_name', 'full_dataset_name', 'dataset_type', 'dataset_version',
#        'team_name', 'team', 'collection', 'collection_name',
#        'collection_version', 'workflow', 'raw_bucket_name', 'dev_bucket_name',
#        'uat_bucket_name', 'prod_bucket_name', 'cde_version', 'grouping',
#        'cohort', 'latest_release', 'release_type']

new_ds_names = [
    "jakobsson-pmdbs-bulk-rnaseq",
    # "wood-pmdbs-multimodal-seq"
    # "voet-pmdbs-sn-multimodal-parsebio",
    ]

ds_type_mapper = {   
    "jakobsson-pmdbs-bulk-rnaseq": "pmdbs-bulk-rnaseq",
    # "wood-pmdbs-multimodal-seq": "pmdbs-multimodal",
    # "voet-pmdbs-sn-multimodal-parsebio": "pmdbs-multimodal",
}

grouping = "pmdbs-other"

ds_table =pd.DataFrame()
ds_table['dataset_name'] = new_ds_names
ds_table['full_dataset_name'] = "team-" + ds_table["dataset_name"]
ds_table['dataset_type'] = ds_table['dataset_name'].map(ds_type_mapper)
ds_table['dataset_version'] = "v1.0"
ds_table['team_name'] = ds_table["full_dataset_name"].apply(lambda x: "-".join(x.split("-")[:2]))
ds_table['team'] = ds_table["team_name"].apply(lambda x: x.split("-")[-1])

ds_table["collection"] = "NA"
ds_table["collection_name"] = "NA"
ds_table["collection_version"] = "NA"
ds_table["workflow"] = "NA"

# set bucket names here note "prod" bucket is named -> "curated"
ds_table["raw_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-raw-{x}"
)
ds_table["dev_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-dev-{x}"
)
ds_table["uat_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-uat-{x}"
)
ds_table["prod_bucket_name"] = ds_table["full_dataset_name"].apply(
    lambda x: f"gs://asap-curated-{x}"
)

ds_table["cde_version"] = current_cde
ds_table["grouping"] = grouping
ds_table["cohort"] = False
ds_table["latest_release"] = current_release
ds_table["release_type"] = "Urgent"

# need to add collection versions.
collection_versions = {
    "pmdbs-sc-rnaseq" : "v2.0.0",
    "pmdbs-bulk-rnaseq" : "v1.0.0",
    "NA":"NA"
}
ds_table["collection_version"] = ds_table["collection"].map(collection_versions)



# %%
ds_tables.append(ds_table)

datasets_table = pd.concat(ds_tables).reset_index(drop=True)
# need to drop "cde_bucket_path"

# %%
###################
### MAJOR RELEASE UPDATES
###################

release_version = "v2.0.0"
collections = ["pmdbs-sc-rnaseq", "pmdbs-bulk-rnaseq", "pmdbs-spatial", "mouse-spatial", "mouse-sc-rnaseq"]
groupings = ["pmdbs-sc-rnaseq", "pmdbs-bulk-rnaseq", "pmdbs-spatial", "mouse-spatial", "mouse-other", "pmdbs-other"]
# grouping = [
#     "pmdbs-sc-rnaseq", 
#     "pmdbs-bulk-rnaseq",
#     "mouse-other",
#     "mouse-other",
#     "pmdbs-other",
#     # "pmdbs-other",
#     # "pmdbs-other",
#     ]

collection_name = {
    "pmdbs-sc-rnaseq" : "PMDBS scRNAseq",
    "pmdbs-bulk-rnaseq" : "PMDBS bulkRNAseq",
    "pmdbs-spatial" : "PMDBS Spatial RNAseq",
    "mouse-spatial" : "Mouse Spatial RNAseq",
    # "mouse-sc-rnaseq" : "Mouse scRNAseq",
    # "mouse-bulk-rnaseq" : "Mouse bulkRNAseq",
    # "pmdbs-other" : "Other PMDBS Data"
}

# need to add collection versions.
collection_versions = {
    "pmdbs-sc-rnaseq" : "v3.0.0",
    "pmdbs-bulk-rnaseq" : "v1.1.0",
    "pmdbs-spatial" : "v1.0.0",
    "mouse-spatial" : "v1.0.0",
    # "mouse-sc-rnaseq" : "v1.0.0",
    # "mouse-bulk-rnaseq" : "v1.0.0",
    "NA":"NA"
}



# pmdbs-sc-rnaseq-wf
#     hafler-pmdbs-sn-rnaseq-pfc
#     lee-pmdbs-sn-rnaseq
#     jakobsson-pmdbs-sn-rnaseq
#     scherzer-pmdbs-sn-rnaseq-mtg
#     hardy-pmdbs-sn-rnaseq

# pmdbs-bulk-rnaseq-wf 
#     hardy-pmdbs-bulk-rnaseq
#     lee-pmdbs-bulk-rnaseq-mfg
#     wood-pmdbs-bulk-rnaseq

# spatial-transcriptomics-wf Nanostring GeoMx
#     edwards-pmdbs-spatial-geomx-th

# spatial-transcriptomics-wf 10x Visium 
#     cragg-mouse-sn-rnaseq-striatum

# drop dataset_name = jakobsson-pmdbs-sn-rnaseq , dataset_version = v1.0 row
datasets_table = datasets_table[~((datasets_table["dataset_name"] == "jakobsson-pmdbs-sn-rnaseq") & (datasets_table["dataset_version"] == "v1.0"))]
datasets_table = datasets_table.reset_index(drop=True)

# set collection to pmdbs-sc-rnaseq and workflow to pmdbs_sc_rnaseq
datasets_table.loc[datasets_table["dataset_name"] == "jakobsson-pmdbs-sn-rnaseq", "collection"] = "pmdbs-sc-rnaseq"
datasets_table.loc[datasets_table["dataset_name"] == "jakobsson-pmdbs-sn-rnaseq", "workflow"] = "pmdbs_sc_rnaseq"
# # mapp biederer-mouse-sc-rnaseq and cragg-mouse-sn-rnaseq-striatum to mouse-sc-rnaseq
# datasets_table.loc[datasets_table["dataset_name"] == "biederer-mouse-sc-rnaseq", "collection"] = "mouse-sc-rnaseq"
# datasets_table.loc[datasets_table["dataset_name"] == "cragg-mouse-sn-rnaseq-striatum", "collection"] = "mouse-sc-rnaseq"
# # workflow stays NA

# map cragg-mouse-spatial-visium-striatum to mouse-spatial
datasets_table.loc[datasets_table["dataset_name"] == "cragg-mouse-spatial-visium-striatum", "collection"] = "mouse-spatial"
datasets_table.loc[datasets_table["dataset_name"] == "cragg-mouse-spatial-visium-striatum", "workflow"] = "spatial_visium"
# map edwards-pmdbs-spatial-geomx-th to pmdbs-spatial
datasets_table.loc[datasets_table["dataset_name"] == "edwards-pmdbs-spatial-geomx-th", "collection"] = "pmdbs-spatial"
datasets_table.loc[datasets_table["dataset_name"] == "edwards-pmdbs-spatial-geomx-th", "workflow"] = "spatial_geomx"
# jakobsson and wood multimodal left as NA

# set all cde to v3.2
datasets_table["cde_version"] = "v3.2"
datasets_table["latest_release"] = current_release
# remap collections.
datasets_table["collection_version"] = datasets_table["collection"].map(collection_versions)

datasets_table["collection_name"] = datasets_table["collection"].map(collection_name)

# %%
datasets_table["release_type"] = "Major"

# %%
# bump cohort datasets version number to match collection
# %%
datasets_table.loc[datasets_table["cohort"]==True,"dataset_version"] = datasets_table.loc[datasets_table["cohort"]==True,"collection_version"] 




# %%
###################
### STEP 1b: save datasets.csv
###################
current_release = "v3.0.0"

release_path = root_path / "releases" / current_release



# %%
## build 
if not release_path.exists():
    release_path.mkdir(parents=True)
datasets_table.to_csv(release_path / "new_datasets.csv", index=False)

datasets_table.to_csv(release_path / "datasets.csv", index=False)



# %%
### TODO:  add code to copy the metadata for all "new" datasets from github 
#.  NOTE:  for a Major release ALL the datasets are "new"

metadata_root = root_path.parent / "asap-crn-cloud-dataset-metadata"
datasets_path = metadata_root / "datasets"
map_path = metadata_root / "asap-ids/master"

last_release = "v3.0.0"
release_path_base = root_path / "releases" / last_release
datasets_table = pd.read_csv(release_path_base / "datasets.csv")

pmbds_tables = ['STUDY',
 'PROTOCOL',
 'SUBJECT',
 'SAMPLE',
 'ASSAY_RNAseq',
 'DATA',
 'PMDBS',
 'CLINPATH',
 'CONDITION']

for row, dataset in datasets_table.iterrows():

    # for dataset in datasets["dataset_name"]:
    dataset_name = dataset["dataset_name"]
    ds_path = datasets_path / dataset_name
    

    release_ds_path = release_path / "datasets" / dataset_name

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]
    latest_release = dataset["latest_release"]

    # type
    # source = 
    ds_source = dataset["dataset_type"]
    # cohort
    cohort = dataset["cohort"]

    # type

    spatial = "spatial" in dataset_name
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = pmbds_tables.copy()
        if spatial:
            table_names.append("SPATIAL")
            print("added spatial")
        else:
            print("no spatial")
            print(table_names)
        # table_names = table_names.append("SPATIAL") if spatial else table_names
    elif ("mouse" in ds_source):
        source = "mouse"
        table_names = MOUSE_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")
            print("added spatial")
        else:
            print("no spatial")
            print(table_names)

        # table_names = table_names.append("SPATIAL") if spatial else table_names
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

        source_mdata_path = ds_path / "metadata" / "release" / latest_release
        if not source_mdata_path.exists():
            print(f"making {dataset_name} as {source_mdata_path} does not exist")
            source_mdata_path.mkdir(parents=True)
x
        export_meta_tables(dfs, source_mdata_path)
        write_version(schema_version, source_mdata_path / "cde_version")
        write_version(dataset_version, source_mdata_path / "dataset_version")

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

# %%
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
        if package_source.exists():
            shutil.copytree(package_source, package_destination, dirs_exist_ok=True)
        else:
            print(f"Skipping DOI copy for {collection} as {package_source} does not exist")




#%% 
#.  archive CDE 


resource_path = root_path / "resource"

for schema in datasets_table["cde_version"].unique():
    archive_CDE(schema, resource_path)




# %%
# updated GCP URIs in
# collect curated_files.csv from all releases and combine
#
#. - metadata/DATA.csv
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

def gen_dev_bucket_summary(
    dev_bucket_name: str, workflow_name: str, dl_path: Path, dataset_name: str, flatten: bool = False
) -> list[str] :

    bucket_path = (
        f"{dev_bucket_name.split('/')[-1]}"  # dev_bucket_name has gs:// prefix
    )
    print(f"Processing {bucket_path}")
    ## OTHER and everything else...
    # create a list of the curated files in /cohort_analysis
    if workflow_name == "NA":
        print(f"Skipping {dataset_name} as workflow is NA")
        return []
    elif workflow_name in ["pmdbs_bulk_rnaseq", "pmdbs_sc_rnaseq","spatial_geomx", "spatial_visium"]: # "pmdbs_bulk_rnaseq":
        prefix = f"{workflow_name}/**"
        # also downstream + upstream
    else:
        print(f"Skipping {dataset_name} as workflow {workflow_name} is not implemented")
        return []

    project = "dnastack-asap-parkinsons"
    project = None
    artifacts = gsutil_ls2(bucket_path, prefix, project=project)
    # drop empty strings, files that start with ".", and folders
    artifact_files = [
        f for f in artifacts if f != "" and Path(f).name[0] != "." and f[-1] != "/"
    ]
    return artifact_files
# %%



all_curated_files = {}
for row, dataset in datasets_table.iterrows():

    # for dataset in datasets["dataset_name"]:
    dataset_name = dataset["dataset_name"]
    ds_path = datasets_path / dataset_name
    

    release_ds_path = release_path / "datasets" / dataset_name

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]
    latest_release = dataset["latest_release"]

    dataset = dataset.fillna("NA")
    workflow = dataset["workflow"]

    print(f"Processing {dataset_name}")
    dev_bucket_name = dataset["dev_bucket_name"]
    dl_path = release_ds_path / "file_metadata"
    curated_files = gen_dev_bucket_summary(dev_bucket_name, workflow, dl_path, dataset_name)    

    all_curated_files[dataset_name] = curated_files

# %%
# do it in parts to deal with messed up MANIFEST.tsv
# for dataset_name, curated_files in all_curated_files.items():
for row, dataset in datasets_table.iterrows():
    dataset_name = dataset["dataset_name"]
    curated_files = all_curated_files[dataset_name]
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
        local = release_path / "datasets" / dataset_name / "file_metadata" / f"{row["file_path"].replace('/', '-')}-{row['filename']}"  
        gsutil_cp2(remote, local, directory=False)
        print(f"Downloaded {remote} to {local}")

# %%
for row, dataset in datasets_table.iterrows():
    dataset_name = dataset["dataset_name"]
    curated_files = all_curated_files[dataset_name]

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
        local = release_path / "datasets" / dataset_name / "file_metadata" / f"{row["file_path"].replace('/', '-')}-{row['filename']}"  
        # gsutil_cp2(remote, local, directory=False)
        # print(f"Downloaded {remote} to {local}")
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
    assert (curated_files_df.loc[curated_files_df["workflow_y"].notna(), "workflow"] == curated_files_df.loc[curated_files_df["workflow_y"].notna(), "workflow_y"]).all(), "workflow and workflow_y do not match"

    # add ASAP_dataset_id and ASAP_team_id to curated_files_df
    # get it by reading the metadata/STUDY.csv
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
    curated_files_df["gcp_uri"] = dataset["prod_bucket_name"] + "/" + curated_files_df["file_path"]
    curated_files_df["ASAP_dataset_id"] = ASAP_dataset_id
    curated_files_df["ASAP_team_id"] = ASAP_team_id
    curated_files_df = curated_files_df[input_cols]

    # save curated_files_df to file_metadata/curated_files.csv
    curated_files_df.to_csv(release_path / "datasets" / dataset_name / "file_metadata/curated_files.csv", index=False)


# %%
# copy release_path / "datasets" / dataset_name / "file_metadata/curated_files.csv" to raw bucket...


for row, dataset in datasets_table.iterrows():
    dataset_name = dataset["dataset_name"]
    # team name
    team_name = dataset["team_name"].lstrip("team-")


    # key_file_path = Path.home() / f"Projects/ASAP/{team_name}-credentials.json"
    # res = authenticate_with_service_account(key_file_path)


    curated_files_csv = release_path / "datasets" / dataset_name / "file_metadata/curated_files.csv"
    raw_bucket_name = dataset["raw_bucket_name"]
    destination = f"{raw_bucket_name}/file_metadata/curated_files.csv"
    gsutil_cp(curated_files_csv, destination, directory=False)
    print(f"Uploaded {curated_files_csv} to {destination}")


# %%
# sync the release folder to the release bucket

platform_bucket = "gs:/asap-crn-cloud-release-resources"


# %%
gsutil -m rsync -dr "releases/" "gs://asap-crn-cloud-release-resources/releases/"

gsutil cp -r "gs://asap-crn-cloud-release-resources/releases/v3.0.0/datasets/*"  "gs://asap-crn-cloud-release-resources/ 
gsutil -m cp -r "releases/v3.0.0/collections" "gs://asap-crn-cloud-release-resources/"  


## warning these clobber things...
# gsutil -m rsync -dr "gs://asap-crn-cloud-release-resources/releases/v3.0.0/datasets/" "gs://asap-crn-cloud-release-resources/"
# gsutil -m rsync -dr "gs://asap-crn-cloud-release-resources/releases/v3.0.0/collections/" "gs://asap-crn-cloud-release-resources/collections/"

echo "v3.0.0" | gsutil cp - gs://asap-crn-cloud-release-resources/version
gsutil -m cp "resource/*.csv" "gs://asap-crn-cloud-release-resources/CDE/"


