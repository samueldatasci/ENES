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
from ImportUtils import get_dfGeo_from_MDB, get_dfSchools_from_MDB, get_dfCursos_from_MDB, get_dfExames_from_MDB, get_dfResultados_from_MDB
from ImportUtils import get_dfGeo_from_parquet, get_dfSchools_from_parquet, get_dfCursos_from_parquet, get_dfExames_from_parquet, get_dfResultados_from_parquet
from ImportUtils import get_dfSitFreq_from_parquet, get_dfSitFreq_from_MDB
from ImportUtils import download_zip_files, extract_MDBs, get_dfAll_from_datasets, get_dfAll_from_parquet
from ImportUtils import get_dfAllFase1_from_parquet, get_dfInfoEscolas_from_parquet, get_dfAllFase1_from_datasets, get_dfInfoEscolas_from_datasets
from ImportUtils import get_dfInfoEscolas_for_analysis_from_datasets, get_dfInfoEscolas_for_analysis_from_parquet

import warnings
#endregion imports


# Specify the file path where you want to save the YAML data
if __name__ == '__main__':
	path_dicts_params = "./code/"
else:
	path_dicts_params = ""

yaml_file = 'novaenes.yaml'

# Read the data from the YAML file
with open(yaml_file, 'r') as file:
   dataParams = yaml.load(file, Loader=yaml.FullLoader)

# Extract the individual dictionaries from the loaded data
dicFiles = dataParams['dicFiles']
dicParquetBase = dataParams['dicParquetBase']
dicParquetExtra = dataParams['dicParquetExtra']
dicParams = dataParams['dicParams']
dicNuts2 = dataParams['dicNuts2']
dicExamShortNames = dataParams['dicExamShortNames']
dicSaveDataframeAsCSV = dataParams['dicSaveDataframeAsCSV']
dicFilters = dataParams["dicFilters"]
dicAnalysis = dataParams["dicAnalysis"]

#print(dicAnalysis)


if dicParams["ignore_known_warnings"] == True:
	warnings.filterwarnings('ignore')


def it():

	print("Running...")
	current_time = vprint_time(start_time=0, prefix='Starting...')


	parquetPath = dicParams['dataFolderParquet']
	createParquetBase = False
	if not exists(parquetPath):
		print("Parquet directory", parquetPath, "does not exist; creating it")
		mkdir(parquetPath)
		print("Creating all Base parquet files")
		createParquetBase = True
	else:
		for key in dicParquetBase:
			file = parquetPath + dicParquetBase[key] + ".parquet"
			if not dicParams["parquetCompression"] == None:
				file = file + "." + dicParams["parquetCompression"]
			if not exists(file):
				print("Parquet base file does not exist; creating it: " + file)
				print("Creating all Parquet base files from MDB")
				createParquetBase = True
				break
	
	if createParquetBase == True:

		# Check if MDB files exist
		MDBPath = dicParams['dataFolderMDB']
		createMDB = False
		if not exists(MDBPath):
			print("MDB directory", MDBPath, "does not exist; creating it")
			mkdir(MDBPath)
			print("Extracting all MDB files")
			createMDB = True
		else:
			for key in dicFiles:
				key = key.replace(".zip", ".mdb")
				file = MDBPath + key
				if not exists(file):
					print("MDB file does not exist: " + file)
					print("Creating all MDB files from ZIP files")
					createMDB = True
					break
			
		if createMDB == True:
		
			zipPath = dicParams['dataFolderZIP']
			createZIP = False
			if not exists(dicParams['dataFolderZIP']):
				print("ZIP directory", zipPath, "does not exist; creating it")
				mkdir(zipPath)
				print("Downloading all ZIP files")
				createZIP = True
			else:
				for key in dicFiles:
					file = zipPath + key
					if not exists(file):
						print("ZIP file does not exist: " + file)
						print("Downloading all ZIP files")
						createZIP = True
						break

			if createZIP == True:
				download_zip_files()

			extract_MDBs()

		# Create Parquet files
		print("Creating all Parquet base files from MDB. This will also load all base dataframes")

		dfGeo           = get_dfGeo_from_MDB()
		current_time = vprint_time(current_time, 'Loaded dfGeo from MDB. Shape: ' + str(dfGeo.shape) + '...')
		dfSchools       = get_dfSchools_from_MDB()
		current_time = vprint_time(current_time, 'Loaded dfSchools from MDB. Shape: ' + str(dfSchools.shape) + '...')
		dfCursos        = get_dfCursos_from_MDB()
		current_time = vprint_time(current_time, 'Loaded dfCursos from MDB. Shape: ' + str(dfCursos.shape) + '...')
		dfExames        = get_dfExames_from_MDB()
		current_time = vprint_time(current_time, 'Loaded dfExames from MDB. Shape: ' + str(dfExames.shape) + '...')
		dfResultados    = get_dfResultados_from_MDB()
		current_time = vprint_time(current_time, 'Loaded dfResultados from MDB. Shape: ' + str(dfResultados.shape) + '...')
		dfSitFreq    = get_dfSitFreq_from_MDB()
		current_time = vprint_time(current_time, 'Loaded dfSitFreq from MDB. Shape: ' + str(dfSitFreq.shape) + '...')

	else:
		# Because parquet base files already exist, they were not created now
		# Therefore, we need to load the dataframes from disk
		# If createParquetBase == True, then the datasets were loaded when we were creating them

		print("Loading all base dataframes from Parquet base files on disk")

		dfGeo        = get_dfGeo_from_parquet()
		dfSchools    = get_dfSchools_from_parquet()
		dfCursos     = get_dfCursos_from_parquet()
		dfExames     = get_dfExames_from_parquet()
		dfResultados = get_dfResultados_from_parquet()
		dfSitFreq    = get_dfSitFreq_from_parquet()


	#parquetPath = dicParams['dataFolderParquet']
	if createParquetBase == True:
		# If we created the parquet base files, we need to create the parquet extra files
		createParquetExtra = True
	else:
		createParquetExtra = False
		for key in dicParquetExtra:
			file = parquetPath + dicParquetExtra[key] + ".parquet.gzip"
			if not exists(file):
				print("Parquet extra file", file, " does not exist. Creating all Parquet extra files from existing parquet files")
				createParquetExtra = True
				break
	
	if createParquetExtra == True:

		print("Creating Extra parquet files from dataframes")

		dfAll = get_dfAll_from_datasets(dfGeo, dfSchools, dfCursos, dfExames, dfResultados, dfSitFreq)
		current_time = vprint_time(current_time, 'Created dfAll from datasets. Shape: ' + str(dfAll.shape) + '...')

		dfAllFase1 = get_dfAllFase1_from_datasets(dfAll)
		current_time = vprint_time(current_time, 'Created dfAllFase1 from datasets. Shape: ' + str(dfAllFase1.shape) + '...')

		dfInfoEscolas = get_dfInfoEscolas_from_datasets(dfAllFase1)
		current_time = vprint_time(current_time, 'Created dfInfoEscolas from datasets. Shape: ' + str(dfInfoEscolas.shape) + '...')

		dfInfoEscolas_for_analysis = get_dfInfoEscolas_for_analysis_from_datasets(dfInfoEscolas)
		current_time = vprint_time(current_time, 'Created dfInfoEscolas_for_analysis from datasets. Shape: ' + str(dfInfoEscolas_for_analysis.shape) + '...')

	else:
		# Because parquet extra files already exist, just load them from disk

		print("Loading Extra parquet files from disk")

		dfAll         = get_dfAll_from_parquet()
		dfAllFase1    = get_dfAllFase1_from_parquet()
		dfInfoEscolas = get_dfInfoEscolas_from_parquet()
		dfInfoEscolas_for_analysis = get_dfInfoEscolas_for_analysis_from_parquet()


if __name__ == '__main__':
	myclk = vprint_time( start_time= 0, prefix= '>>> Starting...')
	it()
	myclk = vprint_time(start_time=myclk, prefix='>>> Finished...')
	