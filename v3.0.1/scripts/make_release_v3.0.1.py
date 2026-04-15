
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

# warning NEW LOCATION
root_path = Path.cwd().parent.parent.parent


# %%
# TODO:  these should be read from the github repos in the future..
##################
# STEP 0:  Collect Previously platformed datasets
##################
# ALL DATASETS

last_release = "v3.0.0"

release_path_base = root_path / "releases" / last_release

datasets_table = pd.read_csv(release_path_base / "datasets.csv")

ds_table = pd.read_csv(release_path_base / "datasets.csv")
ds_table.fillna("NA", inplace=True)

ds_tables = [ds_table]


# %%
##################
# STEP 1a:  Add new datasets ("Urgent")
##################
current_release = "v3.0.1"
current_cde = "v3.3"

# col_names = ['dataset_name', 'full_dataset_name', 'dataset_type', 'dataset_version',
#        'team_name', 'team', 'collection', 'collection_name',
#        'collection_version', 'workflow', 'raw_bucket_name', 'dev_bucket_name',
#        'uat_bucket_name', 'prod_bucket_name', 'cde_version', 'grouping',
#        'cohort', 'latest_release', 'release_type']


##### treat the Scherzer version bumps NOT as new_datasets. The non-hybsel was just curated with the Major release...
    # "scherzer-pmdbs-sn-rnaseq-mtg", #v1.1
    # "scherzer-pmdbs-sn-rnaseq-mtg-hybsel", #v1.1
    # "scherzer-pmdbs-sn-rnaseq-mtg": "pmdbs-sn-rnaseq", #v1.1
    # "scherzer-pmdbs-sn-rnaseq-mtg-hybsel": "pmdbs-other", #v1.1


new_ds_names = [
    "alessi-invitro-ms-p-hek293-gtip",
    "jakobsson-invitro-bulk-rnaseq-dopaminergic",
    "jakobsson-invitro-bulk-rnaseq-microglia",
    # "rio-ipsc-bulk-rnaseq", 
    "schlossmacher-mouse-sn-rnaseq-osn-aav-transd",
    "wood-pmdbs-multimodal-seq",
    # "voet-pmdbs-sn-multimodal-parsebio",
    # "voet-pmdbs-sn-atac",
    "scherzer-pmdbs-spatial-visium-mtg", #v1.0
    "scherzer-pmdbs-genetics",  #v1.0
    "alessi-mouse-sn-rnaseq-dorsal-striatum-g2019s",
    ]

ds_type_mapper = {   
    "alessi-invitro-ms-p-hek293-gtip": "proteomics",
    "jakobsson-invitro-bulk-rnaseq-dopaminergic": "invitro-bulk-rnaseq",
    "jakobsson-invitro-bulk-rnaseq-microglia": "invitro-bulk-rnaseq",
    # "rio-ipsc-bulk-rnaseq": "invitro-bulk-rnaseq",
    "schlossmacher-mouse-sn-rnaseq-osn-aav-transd": "mouse-sc-rnaseq",
    "wood-pmdbs-multimodal-seq": "pmdbs-sn-rnaseq",
    # "voet-pmdbs-sn-multimodal-parsebio" : "pmdbs-multiomic",
    # "voet-pmdbs-sn-atac" : "pmdbs-atac-seq", 
    "scherzer-pmdbs-spatial-visium-mtg": "pmdbs-spatial", #v1.0
    "scherzer-pmdbs-genetics": "pmdbs-genetics",  #v1.0
    "alessi-mouse-sn-rnaseq-dorsal-striatum-g2019s": "mouse-sc-rnaseq", #v1.0"
}

ds_group_mapper = {   
    "alessi-invitro-ms-p-hek293-gtip": "proteomics",
    "jakobsson-invitro-bulk-rnaseq-dopaminergic": "invitro",
    "jakobsson-invitro-bulk-rnaseq-microglia": "invitro",
    # "rio-ipsc-bulk-rnaseq": "invitro",
    "schlossmacher-mouse-sn-rnaseq-osn-aav-transd": "other-mouse",
    "wood-pmdbs-multimodal-seq": "other-pmdbs",
    # "voet-pmdbs-sn-multimodal-parsebio": "other-pmdbs",
    # "voet-pmdbs-sn-atac" : "other-pmdbs", 
    "scherzer-pmdbs-spatial-visium-mtg": "other-pmdbs", #v1.0
    "scherzer-pmdbs-genetics": "other-pmdbs",  #v1.0
    "alessi-mouse-sn-rnaseq-dorsal-striatum-g2019s": "other-mouse", #v1.0"
}


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
ds_table["grouping"] = ds_table["dataset_name"].map(ds_group_mapper)
ds_table["cohort"] = False
ds_table["latest_release"] = current_release
ds_table["release_type"] = "Urgent"

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

ds_table["collection_version"] = ds_table["collection"].map(collection_versions)





# %%

# %%
###################
### STEP 1b: save datasets.csv
###################
current_release = "v3.0.1"

release_path = root_path / "releases" / current_release
## build 
if not release_path.exists():
    release_path.mkdir(parents=True)
# ds_table.to_csv(release_path / "new_datasets.csv", index=False)


new_datasets = ds_table["dataset_name"].tolist()

# %%


###################
# %%
ds_tables.append(ds_table)

datasets_table = pd.concat(ds_tables).reset_index(drop=True)
# fix the v1.1 version bumps
    # "scherzer-pmdbs-sn-rnaseq-mtg": "pmdbs-sn-rnaseq", #v1.1
    # "scherzer-pmdbs-sn-rnaseq-mtg-hybsel": "other-pmdbs", #v1.1
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg", "dataset_version"] = "v1.1"
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg-hybsel", "dataset_version"] = "v1.1"
# fix the cde number
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg", "cde_version"] = "v3.3"
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg-hybsel", "cde_version"] = "v3.3"
# fix latest_release & release_type

datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg", "latest_release"] = "v3.0.1"
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg-hybsel", "latest_release"] = "v3.0.1"
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg", "release_type"] = "Urgent"
datasets_table.loc[datasets_table["dataset_name"] == "scherzer-pmdbs-sn-rnaseq-mtg-hybsel", "release_type"] = "Urgent"

datasets_table.to_csv(release_path / "datasets.csv", index=False)


# # %% 

# this is backwards but will result in the right thing...
new_datasets = new_datasets + ["scherzer-pmdbs-sn-rnaseq-mtg", "scherzer-pmdbs-sn-rnaseq-mtg-hybsel"]

new_datasets_table = datasets_table[datasets_table["dataset_name"].isin(new_datasets)]

new_datasets_table.to_csv(release_path / "new_datasets.csv", index=False)

# %%
### TODO:  add code to copy the metadata for all "new" datasets from github 
#.  NOTE:  for a Major release ALL the datasets are "new"
current_release = "v3.0.1"
release_path = root_path / "releases" / current_release
## build 
metadata_root = root_path.parent / "asap-crn-cloud-dataset-metadata"
datasets_path = metadata_root / "datasets"
map_path = metadata_root / "asap-ids/master"

last_release = "v3.0.1"
release_path_base = root_path / "releases" / last_release
datasets_table = pd.read_csv(release_path_base / "datasets.csv")
# %%
# pmbds_tables = ['STUDY',
#  'PROTOCOL',
#  'SUBJECT',
#  'SAMPLE',
#  'ASSAY_RNAseq',
#  'DATA',
#  'PMDBS',
#  'CLINPATH',
#  'CONDITION']

for row, dataset in datasets_table.iterrows():

    if dataset["team_name"] != 'team-jakobsson':
        print(f"Skipping {dataset['dataset_name']} as not team-jakobsson")
        continue


    print(f"Processing {dataset['dataset_name']}")
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
    # if not cohort:
    #     print(f"Skipping {dataset['dataset_name']} as not cohort")
    #     continue

    # type
    proteomics = False
    spatial = "spatial" in dataset_name
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = PMDBS_TABLES.copy()
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
    elif ("invitro" in ds_source):
        source = "invitro"
        table_names = CELL_TABLES.copy()

    elif ("proteomics" in ds_source):
        # TODO... there will be non-cell based proteomics...
        source = "invitro"
        proteomics = True
        table_names = PROTEOMICS_TABLES.copy()

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

        # load the metadata and generate cohort stats
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
        dfs = get_crn_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, proteomics=proteomics, source=source)
    
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

        # generate stats for the dataset
        # note PROTEOMICS are mapped to "cell" through "invitro"
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
# Testing for stats discrepancies below
for row, dataset in datasets_table.iterrows():

    # if dataset["team_name"] != 'team-schlossmacher':
    #     print(f"Skipping {dataset['dataset_name']} as not team-alessi")
    #     continue

    # if dataset["dataset_name"] != 'scherzer-pmdbs-spatial-visium-mtg':
    #     print(f"Skipping {dataset['dataset_name']} ")
    #     continue
    print(f"Processing {dataset['dataset_name']}")
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
    table_names = []
    proteomics = False
    spatial = "spatial" in dataset_name
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = PMDBS_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")
            print("added spatial")
        else:
            print("no pmdbs spatial")
            print(table_names)
        # table_names = table_names.append("SPATIAL") if spatial else table_names
    elif ("mouse" in ds_source):
        source = "mouse"
        table_names = MOUSE_TABLES.copy()
        if spatial:
            table_names.append("SPATIAL")
            print("added mouse spatial")
        else:
            print("no mouse spatial")


        # table_names = table_names.append("SPATIAL") if spatial else table_names
    elif ("invitro" in ds_source):
        source = "cell"
        table_names = CELL_TABLES.copy()
    elif ("proteomics" in ds_source):
        # TODO... there will be non-cell based proteomics...
        source = "invitro"
        proteomics = True
        table_names = PROTEOMICS_TABLES.copy()
    else:
        print(f"no source defined for {ds_source}")
        source = ds_source

    final_metadata_path = release_ds_path / "metadata" 
    dfs = load_tables(final_metadata_path, table_names)

    if cohort:
        # get cohort stats 
        # only PMDBS at this point
        print(f"warning, cohort stats only defined for PMDBS") 
        report,df = get_cohort_stats_table(dfs, source)
      
    else:
        # note PROTEOMICS are mapped to "cell" through "invitro"
        report,df = get_stats_table(dfs, source)

    # ### check df and stats

    # if source == "pmdbs":
    #     if report["samples"]["n_samples"] != df['condition_id'].value_counts().sum():
    #         print(f"WARNING: {dataset_name} report N {report["subject"]["n_subjects"]} != df N {df['condition_id'].value_counts().sum()}")

    # elif source == "mouse":
    #     if report["N"] < df.shape[0]:
    #         print(f"WARNING: {dataset_name} report N {report['N']} != df N {df['condition_id'].value_counts().sum()}")
    
    # elif source in [ "cell", "invitro", "proteomics"]:
    #     if proteomics:
    #         if report["N"] < df.shape[0]:
    #             print(f"WARNING: {dataset_name} report N {report['N']} < df N {df.shape[0]}")
    #     else:
    #         if report["N"] < df.shape[0]:
    #             print(f"WARNING: {dataset_name} report N {report['N']} < df N {df.shape[0]}")




#%% 
#  archive CDE 
resource_path = root_path / "resource"

for schema in datasets_table["cde_version"].unique():
    archive_CDE(schema, resource_path)


###
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






















# %% 
# script for updating dataset metadata for a Major/Minor re-release 


# %%
import pandas as pd
from pathlib import Path
import os, sys

from crn_utils.util import (
    read_CDE,
    NULL,
    prep_table,
    read_meta_table,
    read_CDE_asap_ids,
    export_meta_tables,
    load_tables,
    write_version,
)
from crn_utils.asap_ids import *
from crn_utils.validate import validate_table, ReportCollector, process_table

from crn_utils.constants import *
from crn_utils.file_metadata import gen_raw_bucket_summary, update_data_table_with_gcp_uri
from crn_utils.doi import *
from crn_utils.file_metadata import gen_raw_bucket_summary, update_data_table_with_gcp_uri
from crn_utils.release_util import create_metadata_package, prep_release_metadata

%load_ext autoreload
%autoreload 2


# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"




# # %%
# root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata/datasets")
# datasets_path = root_path


# # %%
# import pandas as pd
# from pathlib import Path
# import os, sys


# from util import  NULL,read_meta_table, read_CDE, read_CDE_asap_ids, read_meta_table, export_meta_tables, load_tables
# from asap_ids import *

# from bucket_util import authenticate_with_service_account, gsutil_ls, gsutil_cp, gsutil_mv, gsutil_rsync2

# %load_ext autoreload
# %autoreload 2



# root_path = Path.cwd().parent 

credential_path = Path.home() / f"Projects/ASAP" 
# %%
# STEPS:
# 1. read the release version
# 2. read the datasets for the release
# 3. read the metadata for the datasets
# 4. update the source-of-truth metadata for the release


release_version = "v3.0.0"
# make sure we have a snapshot of the CDEs for the release


release_path = root_path / "releases" / release_version
collections_path = root_path / "collections" / release_version
datasets_path = root_path / "datasets"
datasets_table = pd.read_csv(release_path/"datasets.csv")


# %%


# PMDBS_TABLES = ['STUDY', 'PROTOCOL','SUBJECT', 'ASSAY_RNAseq', 'SAMPLE', 'PMDBS', 'CONDITION', 'CLINPATH', 'DATA']


# %%


schema_path = root_path / "crn-utils/resource/CDE"
schema_version = datasets_table['cde_version'][0]
CDE = read_CDE(schema_version)
asap_ids_df = read_CDE_asap_ids()
# asap_ids_df = read_CDE_asap_ids()
asap_ids_schema = asap_ids_df[["Table","Field"]]

# %%
########### all v2.0 datasets summary #########

# # force order here to make sure we generate consistent IDs... should actually be okay, but just in case
# datasets = [ Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/lee-pmdbs-sn-rnaseq'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/hafler-pmdbs-sn-rnaseq-pfc'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/hardy-pmdbs-sn-rnaseq'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/jakobsson-pmdbs-sn-rnaseq'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/scherzer-pmdbs-sn-rnaseq-mtg'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/sulzer-pmdbs-sn-rnaseq'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/lee-pmdbs-bulk-rnaseq-mfg'),
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/hardy-pmdbs-bulk-rnaseq'), 
#  Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/wood-pmdbs-bulk-rnaseq'),
# Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/scherzer-pmdbs-sn-rnaseq-mtg-hybsel')
#  ]

datasets_all = datasets_table['full_dataset_name'].to_list()

datasets = [ ds for ds in datasets_table['full_dataset_name'] if not ds.startswith('asap-cohort')]
datasets_short = [ ds for ds in datasets_table['dataset_name'] if not ds.startswith('cohort')]

last_team = ""
# for dataset in datasets_short:
#     ds = datasets_table[datasets_table['dataset_name'] == dataset].iloc[0]
#     dataset_name = ds['full_dataset_name']
#     dataset_name_short = ds['dataset_name']

#     dataset_path = datasets_path / dataset_name_short
#     dataset_path.mkdir(parents=True, exist_ok=True)

#     collection = ds['collection']
#     collection_name = ds['collection_name']
#     collection_version = ds['collection_version']
    
#     raw_bucket_name = ds['raw_bucket_name']
#     dev_bucket_name = ds['dev_bucket_name']

#     team_name = ds['team_name']
#     workflow = ds['workflow']

#     print(f"downloading metadat for {dataset_name} from  {raw_bucket_name=}")
#     # we'll get metadata from the raw bucket at "/metadata/release"
#     source = "pmdbs"
#     team = ds['dataset_name'].split('-')[0]
#     bucket = ds['raw_bucket_name']

#     key_file_path = credential_path / f"{team}-credentials.json"

#     if last_team != team:
#         res = authenticate_with_service_account(key_file_path)
#         last_team = team
    
#     file_source = f"{bucket}/metadata"
    
#     metadata_path = dataset_path / "metadata/"
#     metadata_path.mkdir(parents=True, exist_ok=True)

#     destination = f"{metadata_path}/"

#     gsutil_rsync2(file_source, destination)

###  now we ne need to make the release metadata by adding asap_ids to the metadata tables

# %%

map_path = root_path / "asap-ids/master"
suffix = "ids"

retval = load_pmdbs_id_mappers(map_path, suffix)
datasetid_mapper, subjectid_mapper, sampleid_mapper, gp2id_mapper, sourceid_mapper = retval

in_dir = f"metadata/{datasets_table['cde_bucket_path'][0]}"

# # just do the lee-pmdbs-bulk-rnaseq-mfg dataset.  the others are already done
# datasets = [ 
# Path('/Users/ergonyc/Projects/ASAP/asap-crn-metadata/datasets/wood-pmdbs-bulk-rnaseq') 
# ]

datasets = datasets_table['full_dataset_name'].unique()
datasets_short = datasets_table['dataset_name'].unique()
pmdbs_tables = PMDBS_TABLES.copy()

# get datasets for non asap-cohort datasets
datasets = [ ds for ds in datasets_table['full_dataset_name'] if not ds.startswith('asap-cohort')]
datasets_short = [ ds for ds in datasets_table['dataset_name'] if not ds.startswith('cohort')]

for ds,dataset in zip(datasets_short,datasets):
    ds_path = datasets_path / ds
    print(f'Processing {dataset}')
    # ds_path.mkdir(parents=True, exist_ok=True)
    mdata_path = ds_path / in_dir
    tables = [ table for table in mdata_path.iterdir() if table.is_file() and table.suffix == '.csv']


    table_names = [table.stem for table in tables if table.stem in pmdbs_tables]

    
    dfs = load_tables(mdata_path, table_names)

    # we already have all samples mapped
    # # og_DATA = dfs["DATA"].copy()
    # retval = update_pmdbs_id_mappers(dfs["CLINPATH"], 
    #                     dfs["SAMPLE"],
    #                     dataset,
    #                     datasetid_mapper,
    #                     subjectid_mapper,
    #                     sampleid_mapper,
    #                     gp2id_mapper,
    #                     sourceid_mapper)
    # dataset_id_mapper, subjectid_mapper, sampleid_mapper, gp2id_mapper, sourceid_mapper = retval

    dfs = update_pmdbs_meta_tables_with_asap_ids(dfs, 
                                    ds,
                                    asap_ids_schema, 
                                    datasetid_mapper,
                                    subjectid_mapper,
                                    sampleid_mapper,
                                    gp2id_mapper,
                                    sourceid_mapper)


    # export the tables to the metadata directory in a release subdirectory
    out_dir = ds_path / "metadata/release" / release_version
    out_dir.mkdir(parents=True, exist_ok=True)

    export_meta_tables(dfs, out_dir)
    export_meta_tables(dfs, out_dir.parent) # copy to the release directory as well
# %%

# %%

# %%
ASAP_sample_id_tables = asap_ids_schema[asap_ids_schema['Field'] == 'ASAP_sample_id']['Table'].to_list()
ASAP_subject_id_tables = asap_ids_schema[asap_ids_schema['Field'] == 'ASAP_subject_id']['Table'].to_list()

BUCKETS = pd.DataFrame(columns=['ASAP_team_id','ASAP_dataset_id','dataset_name','bucket_name','source','team'])

combined_dfs = {}
for dataset in datasets:
    dataset_name = dataset.name
    print(f'Processing {dataset_name}')
    tables = [ table for table in (dataset / release_dir).iterdir() if table.is_file() and table.suffix == '.csv']

    table_names = [table.stem for table in tables if table.stem in pmdbs_tables]

    dfs = load_tables(dataset / release_dir, table_names)

    release_dir = "metadata/release"

    if combined_dfs == {}: # first time through
        combined_dfs = dfs
    else:
        for tab in pmdbs_tables:
            if tab not in dfs:
                continue
            combined_dfs[tab] = pd.concat([combined_dfs[tab], dfs[tab]], ignore_index=True)

  
    bucket_df = dfs['STUDY'][['ASAP_team_id','ASAP_dataset_id']]
    bucket_df['dataset_name'] = dataset.name
    bucket_df['bucket_name'] = f"gs://asap-raw-team-{dataset.name}"
    bucket_df['source'] = ("-".join(dataset.name.split('-')[2:]))
    bucket_df['team'] = dataset.name.split('-')[0]

    BUCKETS = pd.concat([BUCKETS, bucket_df], ignore_index=True)


# %%
for table,df in combined_dfs.items():
    df.to_csv(metadata_path / f"{table}.csv", index=False)

