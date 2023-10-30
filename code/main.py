# ENV: "Base" (3.10.9)

#global verbose
dicParams = {}
dicFiles = {}
global verbose

#region imports
import pandas as pd
import numpy as np
import yaml
import pyodbc

from os import rename, mkdir
from os.path import exists

# importing the zipfile module
from zipfile import ZipFile
  
# importing the requests module
import requests
import wget

# Import custom library
from ImportUtils import vprint, vprint_time
from ImportUtils import get_dfGeo_from_MDB, get_dfSchools_from_MDB, get_dfCursos_from_MDB, get_dfExames_from_MDB, get_dfResultados_from_MDB, get_dfResultAnalise_from_MDB
from ImportUtils import get_dfGeo_from_Parquet, get_dfSchools_from_Parquet, get_dfCursos_from_Parquet, get_dfExames_from_Parquet, get_dfResultados_from_Parquet, get_dfResultAnalise_from_Parquet
from ImportUtils import firstrun_download_files, firstrun_extract_MDBs, get_dfAll_from_datasets, get_dfAll_from_Parquet

import warnings
#endregion imports


# Specify the file path where you want to save the YAML data
#path_dicts_params = "c:\\ENES\\"
#path_dicts_params = ".\\"
path_dicts_params = ""

# Define dictionaries from the YAML files
with open(path_dicts_params + "Params.yaml", "r") as file:
	dicParams = yaml.load(file, Loader=yaml.FullLoader)
with open(path_dicts_params + "Files.yaml", "r") as file:
	dicFiles = yaml.load(file, Loader=yaml.FullLoader)


if dicParams["ignore_known_warnings"] == True:
	warnings.filterwarnings('ignore')


def bit():
	#get_dfAll_from_datasets()
	from main import dicParams, dicFiles

	dfGeo = get_dfGeo_from_Parquet()
	dfSchools = get_dfSchools_from_Parquet()
	dfExames = get_dfExames_from_Parquet()
	dfResultados = get_dfResultados_from_Parquet()
	dfa = get_dfAll_from_datasets(dfGeo, dfSchools, dfExames, dfResultados)
	#dfa = get_dfAll_from_datasets(dfGeo, dfSchools, dfExames, dfResultados)


def it():
		

	print("Running...")
	current_time = vprint_time(0, 'Starting...')


	# Read parameters from config file
	if dicParams["doDownloadZips"] == True:
		# loading the temp.zip and creating a zip object
		if not exists(dicParams['dataFolderZIP']):
			print("Creating folder " + dicParams['dataFolderZIP'])
			mkdir(dicParams['dataFolderZIP'])
		firstrun_download_files()

	current_time = vprint_time(current_time, 'After download...')

	if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True:
		# loading the temp.zip and creating a zip object
		if not exists(dicParams['dataFolderMDB']):
			print("Creating folder " + dicParams['dataFolderMDB'])
			mkdir(dicParams['dataFolderMDB'])
		firstrun_extract_MDBs()

	current_time = vprint_time(current_time, 'After extract...')


	if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True or dicParams["doCreateDatasets"] == True:
		if not exists(dicParams['dataFolderParquet']):
			print("Creating folder " + dicParams['dataFolderParquet'])
			mkdir(dicParams['dataFolderParquet'])

		vprint("Loading MDB...")
		dfGeo           = get_dfGeo_from_MDB()
		vprint("dfGeo.shape: ", dfGeo.shape)
		current_time = vprint_time(current_time, 'Loaded dfGeo from MDB...')
		dfSchools       = get_dfSchools_from_MDB()
		vprint("dfSchools.shape: ", dfSchools.shape)
		current_time = vprint_time(current_time, 'Loaded dfSchools from MDB...')
		dfCursos        = get_dfCursos_from_MDB()
		vprint("dfCursos.shape: ", dfCursos.shape)
		current_time = vprint_time(current_time, 'Loaded dfCursos from MDB...')
		dfExames        = get_dfExames_from_MDB()
		vprint("dfExames.shape: ", dfExames.shape)
		current_time = vprint_time(current_time, 'Loaded dfExames from MDB...')
		dfResultados    = get_dfResultados_from_MDB()
		vprint("dfResultados.shape: ", dfResultados.shape)
		current_time = vprint_time(current_time, 'Loaded dfResultados from MDB...')
		dfResultAnalise = get_dfResultAnalise_from_MDB()
		vprint("dfResultAnalise.shape: ", dfResultAnalise.shape)
		current_time = vprint_time(current_time, 'Loaded dfResultAnalise from MDB...')
		dfAll = get_dfAll_from_datasets(dfGeo, dfSchools, dfExames, dfResultados)
		vprint("dfAll.shape: ", dfAll.shape)
		current_time = vprint_time(current_time, 'Created dfAll from datasets...')


	else:
		
		# Load datasets from parquet files
		vprint("Loading Parquet...")
		dfGeo           = get_dfGeo_from_Parquet()
		vprint("dfGeo.shape: ", dfGeo.shape)
		current_time = vprint_time(current_time, 'Loaded dfGeo from Parquet...')
		dfSchools       = get_dfSchools_from_Parquet()
		vprint("dfSchools.shape: ", dfSchools.shape)
		current_time = vprint_time(current_time, 'Loaded dfSchools from Parquet...')
		dfCursos        = get_dfCursos_from_Parquet()
		vprint("dfCursos.shape: ", dfCursos.shape)
		current_time = vprint_time(current_time, 'Loaded dfCursos from Parquet...')
		dfExames        = get_dfExames_from_Parquet()
		vprint("dfExames.shape: ", dfExames.shape)
		current_time = vprint_time(current_time, 'Loaded dfExames from Parquet...')
		dfResultados    = get_dfResultados_from_Parquet()
		vprint("dfResultados.shape: ", dfResultados.shape)
		current_time = vprint_time(current_time, 'Loaded dfResultados from Parquet...')
		dfResultAnalise = get_dfResultAnalise_from_Parquet()
		vprint("dfResultAnalise.shape: ", dfResultAnalise.shape)
		current_time = vprint_time(current_time, 'Loaded dfResultAnalise from Parquet...')
		dfAll = get_dfAll_from_Parquet()
		vprint("dfAll.shape: ", dfAll.shape)
		current_time = vprint_time(current_time, 'Created dfAll from Parquet...')

	current_time = vprint_time(current_time, 'After load datasets...')


	print("Loaded...")
	print("dfGeo.shape: ", dfGeo.shape)
	print("dfSchools.shape: ", dfSchools.shape)
	print("dfCursos.shape: ", dfCursos.shape)
	print("dfExames.shape: ", dfExames.shape)
	print("dfResultados.shape: ", dfResultados.shape)
	print("dfResultAnalise.shape: ", dfResultAnalise.shape)

	print("Done!...")



if __name__ == '__main__':
	it()
	# print(1)
	# dfGeo = get_dfGeo_from_Parquet()
	# dfSchools = get_dfSchools_from_Parquet()
	# dfExames = get_dfExames_from_Parquet()
	# print(2)
	# dfResultados = get_dfResultados_from_Parquet()

	# print(3)
	# dfAll = get_dfAll_from_datasets(dfGeo, dfSchools, dfExames, dfResultados)
	# print(4)

	# print("dfResultados.shape: ", dfResultados.shape)
	# print("dfAll.shape: ", dfAll.shape)
	# dfResultados.shape:  (5901336, 18)
	# dfAll.shape:  (5968595, 31)
