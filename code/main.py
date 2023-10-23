# ENV: "Base" (3.10.9)

import pandas as pd
import numpy as np
import yaml
import pyodbc

from os import rename
from os.path import exists

# importing the zipfile module
from zipfile import ZipFile
  
# importing the requests module
import requests
import wget

# Import custom library
from import_enes import *
from utils import vprint, vprint_time
from ImportUtils import get_dfGeo_from_MDB, get_dfSchools_from_MDB, get_dfCursos_from_MDB, get_dfExames_from_MDB, get_dfResultados_from_MDB, get_dfResultAnalise_from_MDB
from ImportUtils import get_dfGeo_from_Parquet, get_dfSchools_from_Parquet, get_dfCursos_from_Parquet, get_dfExames_from_Parquet, get_dfResultados_from_Parquet, get_dfResultAnalise_from_Parquet
from ImportUtils import firstrun_download_files, firstrun_extract_MDBs

global verbose
verbose = True

import warnings
if not verbose:
	warnings.filterwarnings('ignore')


global dicParams, dicFiles

# Specify the file path where you want to save the YAML data
path_dicts_params = "c:\\ENES\\"

# Define dictionaries from the YAML files
dicParams = {}
with open(path_dicts_params + "Params.yaml", "r") as file:
    dicParams = yaml.load(file, Loader=yaml.FullLoader)
dicFiles = {}
with open(path_dicts_params + "Files.yaml", "r") as file:
    dicFiles = yaml.load(file, Loader=yaml.FullLoader)



print("Running...")
current_time = vprint_time(0, 'Starting...')

#global dicParams, dicFiles
#dicParams = yaml.load(open("params.yaml"), Loader=yaml.FullLoader)
#dicFiles = yaml.load(open("files.yaml"), Loader=yaml.FullLoader)

# Read parameters from config file
if dicParams["doDownloadZips"] == True:
    # loading the temp.zip and creating a zip object
	firstrun_download_files()

if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True:
    # loading the temp.zip and creating a zip object
	firstrun_extract_MDBs()


current_time = vprint_time(current_time, 'After download and extract...')
if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True or dicParams["doCreateDatasets"] == True:
	vprint("Loading MDB...")
	dfGeo           = get_dfGeo_from_MDB()
	vprint("dfGeo.shape: ", dfGeo.shape)
	dfSchools       = get_dfSchools_from_MDB()
	vprint("dfSchools.shape: ", dfSchools.shape)
	dfCursos        = get_dfCursos_from_MDB()
	vprint("dfCursos.shape: ", dfCursos.shape)
	dfExames        = get_dfExames_from_MDB()
	vprint("dfExames.shape: ", dfExames.shape)
	dfResultados    = get_dfResultados_from_MDB()
	vprint("dfResultados.shape: ", dfResultados.shape)
	dfResultAnalise = get_dfResultAnalise_from_MDB()
	vprint("dfResultAnalise.shape: ", dfResultAnalise.shape)

else:
	
	# Load datasets from parquet files
	vprint("Loading Parquet...")
	dfGeo           = get_dfGeo_from_Parquet()
	vprint("dfGeo.shape: ", dfGeo.shape)
	dfSchools       = get_dfSchools_from_Parquet()
	vprint("dfSchools.shape: ", dfSchools.shape)
	dfCursos        = get_dfCursos_from_Parquet()
	vprint("dfCursos.shape: ", dfCursos.shape)
	dfExames        = get_dfExames_from_Parquet()
	vprint("dfExames.shape: ", dfExames.shape)
	dfResultados    = get_dfResultados_from_Parquet()
	vprint("dfResultados.shape: ", dfResultados.shape)
	dfResultAnalise = get_dfResultAnalise_from_Parquet()
	vprint("dfResultAnalise.shape: ", dfResultAnalise.shape)

vprint_time(current_time, 'After load datasets...')


print("Loaded...")
print("dfGeo.shape: ", dfGeo.shape)
print("dfSchools.shape: ", dfSchools.shape)
print("dfCursos.shape: ", dfCursos.shape)
print("dfExames.shape: ", dfExames.shape)
print("dfResultados.shape: ", dfResultados.shape)
print("dfResultAnalise.shape: ", dfResultAnalise.shape)

print("Done!...")

