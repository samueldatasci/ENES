import yaml

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

