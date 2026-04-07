
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
from crn_utils.doi import make_pdf_file

%load_ext autoreload
%autoreload 2



# root of asap-crn-cloud-release-resources
root_path = Path.cwd().parent 

# %%
# define root of asap-crn-cloud-dataset-metadata
metadata_root = root_path.parent / "asap-crn-cloud-dataset-metadata"


# %% 
# define 

def make_explorer_card(ds_path: Path, export_path: Path, release_version: str):
    """
    Compose the explorer card for a dataset. 

    """
    # from xhtml2pdf import pisa
    from markdown import markdown

    long_dataset_name = ds_path.name
    team = long_dataset_name.split("-")[0]

    # load jsons
    doi_path = ds_path / "DOI"
    with open(doi_path / f"project.json", "r") as f:
        data = json.load(f)

    title = data.get("title")
    project_title = data.get("project_name")
    project_description = data.get("project_description")
    dataset_title = data.get("dataset_title")
    dataset_description = data.get("dataset_description")
    creators = data.get("creators")
    publication_date = data.get("publication_date")
    version = data.get("version")
    ASAP_lab_name = data.get("ASAP_lab_name")
    PI_full_name = data.get("PI_full_name")
    PI_email = data.get("PI_email")
    submitter_name = data.get("submitter_name")
    submitter_email = data.get("submitter_email")
    publication_DOI = data.get("publication_DOI")
    grant_ids = data.get("grant_ids")
    team_name = data.get("team_name")


    with open(doi_path / "dataset.doi", "r") as f:
        dataset_doi = f.read().strip()

    # load PROTOCOLS
    protocols_path = ds_path / "metadata" / "PROTOCOL.csv"
    protocols_df = pd.read_csv(protocols_path).fillna("NA")
    protocols_github = protocols_df["github_url"].values[0]
    protocols_io = protocols_df["protocols_io_DOI"].values[0]
    # load STUDY
    study_path = ds_path / "metadata" / "STUDY.csv"
    study_df = pd.read_csv(study_path).fillna("NA")
    associated_publication = study_df["publication_DOI"].values[0]

    description = f"**{title}**\n{dataset_description.strip()}"
    readme_content = description
    readme_content += f"\n\n**ASAP Team:** {team_name}\n\n"
    readme_content += f"**Principal Investigator:** {PI_full_name}, {PI_email}\n\n"
    readme_content += f"**Dataset Name:** {ds_path.name}, v{version}\n\n"
    readme_content += f"**Dataset Submitter:** {submitter_name}, {submitter_email}\n\n"

    readme_content += f"\n**Contributors:** "
    for creator in creators:
        readme_content += f"{creator['name']}; "
    readme_content += f"\n"



    readme_content += f"\n**Protocols Github:** {protocols_github}\n\n"
    readme_content += f"**Protocols:** {protocols_io}\n\n"
    readme_content += f"**Dataset DOI:** {dataset_doi}\n\n"
    readme_content += f"**Associated Publication:** {associated_publication}\n\n"

    readme_content += f"**CRN Cloud GitHub:** [https://github.com/ASAP-CRN](https://github.com/ASAP-CRN)\n\n"
    readme_content += f"**CRN Cloud Data Dictionary:** [https://storage.googleapis.com/asap-public-assets/wayfinding/ASAP-CRN-Cloud-Data-Dictionary.pdf](https://github.com/ASAP-CRN)\n\n"
    readme_content += f"**CRN Cloud DOI:** {release_doi}\n\n"
    readme_content += f"**CDE Schema Version:** {schema_version}\n\n"

    readme_content += f"""
Verily Workbench is the preferred environment for conducting analysis on all data collections. Please see the [reference workspace](https://workbench.verily.com/workspaces/asap-crn-reference-workspace) on Verily Workbench for documentation, guides, and sample analyses. Follow the instructions in the workspace overview to get started in your own copy of the reference workspace. When you're familiar with working in your copy of the reference workspace, you may choose to create a clean workspace and add references to this collection in your new workspace by following this link:
[Add to Verily Workbench Workspace](https://workbench.verily.com/add-from-data-collection?dc=c4181f03-6beb-4595-bfa9-d52ba8059e3b&returnUrl=https%3A%2F%2Fcloud.parkinsonsroadmap.org%2Fcollections&returnApp=ASAP%20CRN%20Cloud%20Platform)
  
Note: You must have access to the ASAP CRN Cloud platform before you can utilize the Verily Workbench environment.
 
"""

    # Render as HTML, md, or pdf...
    #
    readme_content_HTML = markdown(readme_content)

    print(f"{long_dataset_name=}")
    print(f"{export_path=}")
    with open(export_path / f"{long_dataset_name}_CARD.md", "w") as f:
        f.write(readme_content)

    make_pdf_file(readme_content_HTML, export_path / f"{long_dataset_name}_CARD.pdf")




# %%
###################
### STEP 1: load datasets.csv
###################
current_release = "v3.0.0"

release_path_base = root_path / "releases" / current_release
datasets_table = pd.read_csv(release_path_base / "datasets.csv")


### STEP 2 define export pathg
export_path = Path.cwd() / "explorer_cards"
if not export_path.exists():
    export_path.mkdir(exist_ok=True)

release_doi = "https://doi.org/10.5281/zenodo.8384742"
### STEP 3 get each dataset's DOI info and create an explorer card

datasets_path = metadata_root / "datasets"

for row, dataset in datasets_table.iterrows():

    # for dataset in datasets["dataset_name"]:
    dataset_name = dataset["dataset_name"]
    ds_path = datasets_path / dataset_name
    
    release_ds_path = release_path_base / "datasets" / dataset_name

    dataset_version = dataset["dataset_version"]
    schema_version = dataset["cde_version"]
    latest_release = dataset["latest_release"]

    # type
    # source = 
    ds_source = dataset["dataset_type"]
    # cohort
    cohort = dataset["cohort"]

    
    if cohort:
        # get cohort stats 
        # only PMDBS at this point
        print(f"warning, cohort datasets inherit their  DOI from the release collection") 

    else:
        # load DOI
        make_explorer_card(ds_path, export_path, release_doi)



# %%
