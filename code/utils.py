# Desc: Utility functions for the project
# Auth: Samuel Santos
# Date: 20-10-15



# importing the requests module
import requests
import wget
import pandas as pd

from os import rename
from os.path import exists


verbose = True

def vprint(*args, **kwargs):
	if verbose:
		print(*args, **kwargs)


import yaml


# Specify the file path where you want to save the YAML data
path_dicts_params = "c:\\ENES\\"


# Define dictionaries from the YAML files

dicParams = {}
with open(path_dicts_params + "Params.yaml", "r") as file:
    dicParams = yaml.load(file, Loader=yaml.FullLoader)

dicFiles = {}
with open(path_dicts_params + "Files.yaml", "r") as file:
    dicFiles = yaml.load(file, Loader=yaml.FullLoader)





def firstrun_download_files()
    # download files in dicFiles from the Internet
    for key in dicFiles.keys():
        if not exists(dicParams['dataFolderZIP'] + key):
            print("Downloading " + key)
            wget.download(dicFiles[key], dicParams['dataFolderZIP'] + key)


def firstrun_extract_MDBs():
    for key in dicFiles.keys():
        zipfile = dicParams['dataFolderZIP'] + key
        
        with ZipFile(zipfile, 'r') as zObject:
            path=dicParams['dataFolderMDB'] + key[:-4] + ".mdb"
 
            filename = ZipFile.namelist(zObject)[0]

            # Extracting specific file in the zip
            # into a specific location.
            zObject.extract( member=filename, path=dicParams['dataFolderMDB'])


            print("Rename {} to {}".format(dicParams['dataFolderMDB'] +filename, dicParams['dataFolderMDB'] + key[:-4] + ".mdb"))
            rename(dicParams['dataFolderMDB'] + filename, dicParams['dataFolderMDB'] + key[:-4] + ".mdb")
            
			


