
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



##################
# "grouping" which describes how the dataset should be grouped in the VWB changed with v3.0.0.  This script corrects the grouping for the v3.0.0 release.

# Other-mouse, Other-pmdbs, pmdbs-spatial, mouse-spatial,pmdbs-sc-rnaseq, pmdbs-bulk-rnaseq

# %%


# %%
###################
### STEP 1b: save datasets.csv
###################
current_release = "v3.0.0"

release_path = root_path / "releases" / current_release


# %%
### TODO:  add code to copy the metadata for all "new" datasets from github 
#.  NOTE:  for a Major release ALL the datasets are "new"

metadata_root = root_path.parent / "asap-crn-cloud-dataset-metadata"
datasets_path = metadata_root / "datasets"
map_path = metadata_root / "asap-ids/master"

last_release = "v3.0.0"
release_path_base = root_path / "releases" / last_release
datasets_table = pd.read_csv(release_path_base / "datasets.csv")


# %%

###################
### MAJOR RELEASE UPDATES
###################

release_version = "v3.0.0"
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
grouping_mapper = {'pmdbs-sc-rnaseq': 'pmdbs-sc-rnaseq',
 'pmdbs-bulk-rnaseq': 'pmdbs-bulk-rnaseq',
 'pmdbs-other': 'other-pmdbs',
 'mouse-sc-rnaseq': 'other-mouse',
 'pmdbs-spatial': 'pmdbs-spatial',
 'mouse-spatial': 'mouse-spatial'}

datasets_table["grouping"] = datasets_table["dataset_type"].map(grouping_mapper)

# now just force jakobsson-pmdbs-bulk-rnaseq to be in other-pmdbs
datasets_table.loc[datasets_table["dataset_name"] == "jakobsson-pmdbs-bulk-rnaseq", "grouping"] = "other-pmdbs"

# %%

datasets_table.to_csv(release_path / "new_datasets.csv", index=False)

datasets_table.to_csv(release_path / "datasets.csv", index=False)



# %%
