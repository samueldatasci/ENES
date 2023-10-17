# ENV: "Base" (3.10.9)

# importing the requests module
import requests
import wget


from os import rename
from os.path import exists

# importing the zipfile module
from zipfile import ZipFile
  
import pandas as pd
import numpy as np
import yaml

import warnings
warnings.filterwarnings('ignore')


# Import custom library
from shared import *
from utils import vprint
from ImportUtils import get_dfGeo_from_MDB, get_dfSchools_from_MDB, get_dfCursos_from_MDB, get_dfExames_from_MDB, get_dfResultados_from_MDB, get_dfResultAnalise_from_MDB
from ImportUtils import get_dfGeo_from_Parquet, get_dfSchools_from_Parquet, get_dfCursos_from_Parquet, get_dfExames_from_Parquet, get_dfResultados_from_Parquet, get_dfResultAnalise_from_Parquet



global verbose

verbose = True

# Read parameters from config file
if dicParams["doDownloadZips"] == True:
    # loading the temp.zip and creating a zip object
	firstrun_download_files()

if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True:
    # loading the temp.zip and creating a zip object
	firstrun_extract_MDBs()
     

if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True or dicParams["doCreateDatasets"] == True:
	dfGeo           = get_dfGeo_from_MDB()
	dfSchools       = get_dfSchools_from_MDB()
	dfCursos        = get_dfCursos_from_MDB()
	dfExames        = get_dfExames_from_MDB()
	dfResultados    = get_dfResultados_from_MDB()
	dfResultAnalise = get_dfResultAnalise_from_MDB()

else:
	
	# Load datasets from parquet files
	dfGeo           = get_dfGeo_from_Parquet()
	dfSchools       = get_dfSchools_from_Parquet()
	dfCursos        = get_dfCursos_from_Parquet()
	dfExames        = get_dfExames_from_Parquet()
	dfResultados    = get_dfResultados_from_Parquet()
	dfResultAnalise = get_dfResultAnalise_from_Parquet()


print("dfResltados: ", dfResultados.shape)
print("dfResultAnalise: ", dfResultAnalise.shape)

print("Done!...")

