#!pip install wget

# importing the requests module
import requests
import wget


from os import rename
from os.path import exists


# importing the zipfile module
from zipfile import ZipFile
  
import pyodbc

import pandas as pd
import numpy as np


# Import custom library
from utils import vprint

import ImportUtils


import warnings
warnings.filterwarnings('ignore')


verbose = True

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



