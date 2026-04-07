
# Need to do additional steps for the v1.0.0 release retroactively... namely...
# add asap_ids to the v2.1 metadata tables
# probably need to add an auxillary function or step to specify if the release is "current"

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

from crn_utils.util import get_dataset_version, write_version, get_release_version, get_cde_version, export_meta_tables, archive_CDE, load_tables
from crn_utils.release_util import create_metadata_package, get_crn_release_metadata, get_stats_table, get_cohort_stats_table
from crn_utils.constants import *

%load_ext autoreload
%autoreload 2

root_path = Path.cwd().parent 

# %%
###################
### STEP -1: make CDE refs, Collection Refs, Dataset Refs via github
###################
# TODO: these should be read from the github repos in the future..

cde_version = "v2.1"
dataset_version = "v1.0"  # all the same for now

###################
### STEP 0.A: define datasets by collections, collection names and datasets
###################
release_version = "v1.0.0"
collections = "pmdbs-sc-rnaseq"

collection_name = {
    "pmdbs-sc-rnaseq" : "PMDBS scRNAseq",
}


collection_versions = {
    "pmdbs-sc-rnaseq" : "v1.0.0",
}

# %%


###########
# define datasets

datasets = pd.DataFrame()

#1 "" 
dataset_type = "pmdbs-sc-rnaseq"
dataset_names = [
        "hafler-pmdbs-sn-rnaseq-pfc",
        "lee-pmdbs-sn-rnaseq",
        "jakobsson-pmdbs-sn-rnaseq",
        "scherzer-pmdbs-sn-rnaseq-mtg",
        ]

datasets['dataset_name'] = dataset_names
datasets['full_dataset_name'] = "team-" + datasets['dataset_name']
datasets['dataset_type'] = dataset_type

# %%

#4) "cohort Data" (synthetic datasets)
dataset_name =  ["cohort-pmdbs-sc-rnaseq"]

add_datasets = pd.DataFrame()
add_datasets['dataset_name'] = dataset_name
add_datasets['full_dataset_name'] = "asap-" + add_datasets['dataset_name']
add_datasets['dataset_type'] = dataset_type

# %%
datasets = pd.concat([datasets, add_datasets])
# %%
datasets = datasets.reset_index(drop=True)

# %%
datasets['dataset_version'] = "v1.0"
datasets['team_name'] = datasets["full_dataset_name"].apply(lambda x: "-".join(x.split("-")[:2]))
datasets['team'] = datasets["team_name"].apply(lambda x: x.split("-")[-1])


# %%
datasets["collection"] = datasets["dataset_type"]
datasets["collection_name"] = datasets["collection"].map(collection_name)
datasets["collection_version"] = datasets["collection"].map(collection_versions)
datasets["workflow"] = datasets["dataset_type"].map({
    "pmdbs-sc-rnaseq" : "pmdbs_sc_rnaseq",
    "pmdbs-bulk-rnaseq" : "pmdbs_bulk_rnaseq"})

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
datasets["grouping"] = "pmdbs-sc-rnaseq"
# datasets["cde_bucket_path"] = None
datasets["cohort"] = datasets["dataset_name"].apply(lambda x: "cohort" in x)
datasets["latest_release"] = release_version
datasets["release_type"] = "Major"

# %%
datasets.loc[datasets["cohort"]==True,"dataset_version"] = datasets.loc[datasets["cohort"]==True,"collection_version"] 

# %%


release_path = root_path / "releases" / release_version 


## export datasets as "new_datasets.csv" and "datasets.csv". This is accurate because all of the datasets got updated curation
datasets.to_csv(release_path / "new_datasets.csv", index=False)

# all datasets are "new"
datasets.to_csv(release_path / "datasets.csv", index=False)



# %%
### TODO:  add code to copy the metadata for all "new" datasets from github 
#.  NOTE:  for a Major release ALL the datasets are "new"

metadata_root = root_path.parent / "asap-crn-cloud-dataset-metadata"
datasets_path = metadata_root / "datasets"
map_path = metadata_root / "asap-ids/master"


last_release = "v1.0.0"
release_path_base = root_path / "releases" / last_release
datasets_table = pd.read_csv(release_path_base / "datasets.csv")

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

    # type
    
    spatial = "spatial" in dataset_name
    if "pmdbs" in ds_source:
        source = "pmdbs"
        table_names = ['SUBJECT', 'SAMPLE','CLINPATH', 'DATA','STUDY', 'PROTOCOL']
    # elif ("mouse" in ds_source):
    #     source = "mouse"
    #     table_names = MOUSE_TABLES
    # elif ("cell" in ds_source):
    #     source = "cell"
    #     table_names = CELL_TABLES
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

        # dfs = load_tables(source_metadata_path, table_names)
        # report,df = get_cohort_stats_table(dfs, source)
        # # collect stats 
        # if report:
        #     # write json report
        #     filename = release_ds_path / "release_stats.json"
        #     with open(filename, "w") as f:
        #         json.dump(report, f, indent=4)
        #     # write stats table
        #     df.to_csv(release_ds_path / "release_stats.csv", index=False)

        # else:
        #     print(f"Skipping stats for {dataset_name}")


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



#%% 
#.  create Collection summary 

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
###################


    ## collect stats 
    #     if source = "pmdbs"
    #         # get pmdbs stats
    #         # MAKE RELEASE METADATA
    

    #         # do joins to get the stats we need.
    #         # first JOIN SAMPLE and CONDITION on "condition_id" how=left to get our "intervention_id" or PD / control
    #         sample_cols = [
    #             "ASAP_sample_id",
    #             "ASAP_subject_id",
    #             "ASAP_team_id",
    #             "ASAP_dataset_id",
    #             "replicate",
    #             "replicate_count",
    #             "repeated_sample",
    #             "batch",
    #             "organism",
    #             "tissue",
    #             "assay_type",
    #             "condition_id",
    #         ]

    #         subject_cols = [
    #             "ASAP_subject_id",
    #             "source_subject_id",
    #             "biobank_name",
    #             "sex",
    #             "age_at_collection",
    #             "race",
    #             "primary_diagnosis",
    #             "primary_diagnosis_text",
    #         ]

    #         pmdbs_cols = [
    #             "ASAP_sample_id",
    #             "brain_region",
    #             "hemisphere",
    #             "region_level_1",
    #             "region_level_2",
    #             "region_level_3",
    #         ]

    #         condition_cols = [
    #             "condition_id",
    #             "intervention_name",
    #             "intervention_id",
    #             "protocol_id",
    #             "intervention_aux_table",
    #         ]

    #         SAMPLE_ = dfs["SAMPLE"][sample_cols]

    #         if "gp2_phenotype" in dfs["SUBJECT"].columns:
    #             subject_cols.append("gp2_phenotype")
    #             SUBJECT_ = dfs["SUBJECT"][subject_cols]
    #         else:
    #             SUBJECT_ = dfs["SUBJECT"][subject_cols]
    #             SUBJECT_["gp2_phenotype"] = SUBJECT_["primary_diagnosis"]

    #         PMDBS_ = dfs["PMDBS"][pmdbs_cols]
    #         CONDITION_ = dfs["CONDITION"][condition_cols]

    #         df = pd.merge(SAMPLE_, CONDITION_, on="condition_id", how="left")

    #         # then JOIN the result with SUBJECT on "ASAP_subject_id" how=left to get "age_at_collection", "sex", "primary_diagnosis"
    #         df = pd.merge(df, SUBJECT_, on="ASAP_subject_id", how="left")

    #         # then JOIN the result with PMDBS on "ASAP_subject_id" how=left to get "brain_region"
    #         df = pd.merge(df, PMDBS_, on="ASAP_sample_id", how="left")

    #         # get stats for the dataset
    #         # 0. total number of samples

    #         age_at_collection = df["age_at_collection"].astype("float")

    #         N = df["ASAP_sample_id"].nunique()

    #         brain_region = (df["brain_region"].value_counts().to_dict(),)
    #         # fill in primary_diagnosis if gp2_phenotype is not in df

    #         PD_status = (df["gp2_phenotype"].value_counts().to_dict(),)
    #         condition_id = (df["condition_id"].value_counts().to_dict(),)
    #         diagnosis = (df["primary_diagnosis"].value_counts().to_dict(),)
    #         sex = (df["sex"].value_counts().to_dict(),)

    #         age = dict(
    #             mean=f"{age_at_collection.mean():.1f}",
    #             median=f"{age_at_collection.median():.1f}",
    #             max=f"{age_at_collection.max():.1f}",
    #             min=f"{age_at_collection.min():.1f}",
    #         )

    #         report = dict(
    #             N=N,
    #             brain_region=brain_region,
    #             PD_status=PD_status,
    #             condition_id=condition_id,
    #             diagnosis=diagnosis,
    #             age=age,
    #             sex=sex,
    #         )

    #         ds_stats[dataset_name] = report

    #         collect_df[dataset_name] = df




# %%
###################

# %%
###################

# %%
###################

# %%
###################

# %%
###################

# %%
###################

# %%
###################

# %%
###################




# %%
###################
### STEP 4: collect stats
###################'
current_release = "v2.0.2"

root_path = Path.cwd().parent 
datasets_path = root_path.parent / "asap-crn-cloud-dataset-metadata/datasets"
metadata_root = datasets_path

stats_dfs, stats = gen_release_artifacts(current_release,root_path, metadata_root)


# %% 
###################
### STEP 5: prepare local release artifacts
###################


def platform_artifacts(
    release_version: str,
    root_path: Path | str | None,
    promotion: str,
    metadata_root: Path | str = None,
):
    """ """
    if root_path is None:
        root_path = DEFAULT_PATH
    else:
        root_path = Path(root_path)

    if metadata_root is None:
        metadata_root = DEFAULT_PATH / f"{METADATA_REPO}/datasets"
    else:
        metadata_root = Path(metadata_root)

    release_path_base = root_path / "releases" / release_version
    artifact_path = release_path_base / "release-artifacts"
    datasets_path = release_path_base / "datasets" / promotion
    collections_path = release_path_base / "collections"

    # metadata_root = root_path / f"{METADATA_REPO}/datasets"
    datasets_table = pd.read_csv(artifact_path / "datasets.csv")

    long_ds_name_mapper = dict(
        zip(datasets_table["dataset_name"], datasets_table["full_dataset_name"])
    )
    # compose a transfer path

    transfer_path = root_path / "transfer"
    transfer_path.mkdir(parents=True, exist_ok=True)
    version_file = transfer_path / release_version
    version_file.touch()

    # copy everything in the datasets_path to the transfer path except datasets_path / * / intermediate
    for dataset_path in datasets_path.iterdir():
        if dataset_path.name == "intermediate":
            continue
        shutil.copytree(
            dataset_path, transfer_path / dataset_path.name, dirs_exist_ok=True
        )
        # remove intermediate from transfer_path
        if (transfer_path / dataset_path.name / "intermediate").exists():
            shutil.rmtree(transfer_path / dataset_path.name / "intermediate")

        # TODO: test. copied from minor release
        # release version for every dataset
        write_version(release_version, f"{destination_path / 'release_version'}")

    # copy artifacts_path to datasets_path
    shutil.copytree(
        artifact_path, transfer_path / "release-artifacts", dirs_exist_ok=True
    )

    # sync everythin in collections and datasets/prod for the release version.
    shutil.copytree(collections_path, transfer_path / "collections", dirs_exist_ok=True)

    # TODO:  fix so it only updates the releases datasets adn copies in
    # now dataset by dataset copy the metadata/release/**.csv to transfer_path / metadata / dataset_name
    for dataset_path in metadata_root.iterdir():
        metadata_release = dataset_path / "metadata" / "release" / release_version
        if (
            metadata_release.exists()
            and dataset_path.name in long_ds_name_mapper.keys()
        ):
            ds_name = long_ds_name_mapper[dataset_path.name]
            shutil.copytree(
                metadata_release,
                transfer_path / ds_name / "metadata",
                dirs_exist_ok=True,
            )

    # create a version file named after the release version
    version_file = metadata_root / release_version
    version_file.mkdir(parents=True, exist_ok=True)
    shutil.copy2(artifact_path / "datasets.csv", version_file)
    shutil.copy2(artifact_path / "new_datasets.csv", version_file)

    # # now rsync the transfer_path to the gs://asap-crn-cloud-release-resources bucket
    # resource_bucket = RELEASE_RESOURCES_BUCKET
    # gsutil_rsync2(transfer_path, resource_bucket)
    # # test_gsutil_rsync2(transfer_path, resource_bucket)

    # # clean up... remove transfer_path
    # # shutil.rmtree(transfer_path)

    return











platform_minor_artifacts(release_version, root_path, promotion, metadata_root)


