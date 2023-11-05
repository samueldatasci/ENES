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
from ImportUtils import get_dfGeo_from_Parquet, get_dfSchools_from_Parquet, get_dfCursos_from_Parquet, get_dfExames_from_Parquet, get_dfResultados_from_Parquet
from ImportUtils import get_dfSitFreq_from_Parquet, get_dfSitFreq_from_MDB
from ImportUtils import download_zip_files, extract_MDBs, get_dfAll_from_datasets, get_dfAll_from_Parquet
from ImportUtils import get_dfAllFase1_from_Parquet, get_dfInfoEscolas_from_Parquet, get_dfAllFase1_from_datasets, get_dfInfoEscolas_from_datasets

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
			file = parquetPath + dicParquetBase[key] + ".parquet.gzip"
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
		print("Creating all Parquet base files from MDB")
		print("This will also load all base dataframes")

		dfGeo           = get_dfGeo_from_MDB()
		print("dfGeo.shape: ", dfGeo.shape)
		current_time = vprint_time(current_time, 'Loaded dfGeo from MDB...')
		dfSchools       = get_dfSchools_from_MDB()
		print("dfSchools.shape: ", dfSchools.shape)
		current_time = vprint_time(current_time, 'Loaded dfSchools from MDB...')
		dfCursos        = get_dfCursos_from_MDB()
		print("dfCursos.shape: ", dfCursos.shape)
		current_time = vprint_time(current_time, 'Loaded dfCursos from MDB...')
		dfExames        = get_dfExames_from_MDB()
		print("dfExames.shape: ", dfExames.shape)
		current_time = vprint_time(current_time, 'Loaded dfExames from MDB...')
		dfResultados    = get_dfResultados_from_MDB()
		print("dfResultados.shape: ", dfResultados.shape)
		current_time = vprint_time(current_time, 'Loaded dfResultados from MDB...')
		dfSitFreq    = get_dfSitFreq_from_MDB()
		print("dfSitFreq.shape: ", dfSitFreq.shape)
		current_time = vprint_time(current_time, 'Loaded dfSitFreq from MDB...')

	else:
		# Because parquet base files already exist, they were not created now
		# Therefore, we need to load the dataframes from disk
		# If createParquetBase == True, then the datasets were loaded when we were creating them

		print("Loading all base dataframes from Parquet base files on disk")

		dfGeo           = get_dfGeo_from_Parquet()
		print("dfGeo.shape: ", dfGeo.shape)
		current_time = vprint_time(current_time, 'Loaded dfGeo from parquet...')
		dfSchools       = get_dfSchools_from_Parquet()
		print("dfSchools.shape: ", dfSchools.shape)
		current_time = vprint_time(current_time, 'Loaded dfSchools from parquet...')
		dfCursos        = get_dfCursos_from_Parquet()
		print("dfCursos.shape: ", dfCursos.shape)
		current_time = vprint_time(current_time, 'Loaded dfCursos from parquet...')
		dfExames        = get_dfExames_from_Parquet()
		print("dfExames.shape: ", dfExames.shape)
		current_time = vprint_time(current_time, 'Loaded dfExames from parquet...')
		dfResultados    = get_dfResultados_from_Parquet()
		print("dfResultados.shape: ", dfResultados.shape)
		current_time = vprint_time(current_time, 'Loaded dfResultados from parquet...')
		dfSitFreq    = get_dfSitFreq_from_Parquet()
		print("dfSitFreq.shape: ", dfSitFreq.shape)
		current_time = vprint_time(current_time, 'Loaded dfSitFreq from parquet...')


	if False:

		#parquetPath = dicParams['dataFolderParquet']
		if createParquetBase == True:
			# If we created the parquet base files, we need to create the parquet extra files
			createParquetExtra = True
		else:
			createParquetExtra = False
			for key in dicParquetExtra:
				file = parquetPath + dicParquetExtra[key] + ".parquet.gzip"
				if not exists(file):
					print("Parquet extra file does not exist; creating it: " + file)
					print("Creating all Parquet extra files from existing parquet files")
					createParquetExtra = True
					break
		
		if createParquetExtra == True:

			print("Creating Extra parquet files from dataframes")

			dfAll = get_dfAll_from_datasets(dfGeo, dfSchools, dfCursos, dfExames, dfResultados, dfSitFreq)
			print("dfAll.shape: ", dfAll.shape)
			current_time = vprint_time(current_time, 'Created dfAll from datasets...')

			dfAllFase1 = get_dfAllFase1_from_datasets(dfAll)
			print("dfAllFase1.shape: ", dfAllFase1.shape)
			current_time = vprint_time(current_time, 'Created dfAllFase1 from datasets...')

			dfInfoEscolas = get_dfInfoEscolas_from_datasets(dfAllFase1)
			print("dfInfoEscolas.shape: ", dfInfoEscolas.shape)
			current_time = vprint_time(current_time, 'Created dfInfoEscolas from datasets...')

		else:
			# Because parquet extra files already exist, just load them from disk

			print("Loading Extra parquet files from disk")

			dfAll = get_dfAll_from_Parquet()

			current_time = vprint_time(current_time, 'Created dfAll from Parquet...')
			dfAllFase1 = get_dfAllFase1_from_Parquet()
			print("dfAllFase1.shape: ", dfAllFase1.shape)

			current_time = vprint_time(current_time, 'Created dfAllFase1 from Parquet...')
			dfAllFase1 = get_dfAllFase1_from_Parquet()
			print("dfAllFase1.shape: ", dfAllFase1.shape)
			
			current_time = vprint_time(current_time, 'Created dfAllFase1 from Parquet...')
			dfInfoEsclas = get_dfInfoEscolas_from_Parquet()
			print("dfInfoEsclas.shape: ", dfInfoEsclas.shape)
			current_time = vprint_time(current_time, 'Created dfInfoEsclas from Parquet...')



if __name__ == '__main__':
	myclk = vprint_time( start_time= 0, prefix= '>>> Starting...')
	it()
	myclk = vprint_time(start_time=myclk, prefix='>>> Finished...')
	