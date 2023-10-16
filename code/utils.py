# Desc: Utility functions for the project
# Auth: Samuel Santos
# Date: 20-10-15


verbose = True

def vprint(*args, **kwargs):
	if verbose:
		print(*args, **kwargs)



import yaml


dicFiles = {}
dicParams = {}
dicParams["FirstRun"] = False
dicParams['dataFolder'] = "C:/ENES/"
dicParams['dataFolderZIP'] = dicParams['dataFolder'] + "ZIP/"
dicParams['dataFolderMDB'] = dicParams['dataFolder'] + "MDB/"
dicParams['dataFolderParquet'] = dicParams['dataFolder'] + "Parquet/"



dicFiles['ENES2008.zip'] =  'https://www.dge.mec.pt/sites/default/files/JNE/enes2008_0.zip'
dicFiles['ENES2009.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2009.zip'
dicFiles['ENES2010.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2010_enes.zip'

dicFiles['ENES2011.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2011_enes.zip'
dicFiles['ENES2012.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2012_enes_secun.zip'
dicFiles['ENES2013.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2013_enes_sec.zip'
dicFiles['ENES2014.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2014.zip'
dicFiles['ENES2015.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2015_0.zip'
dicFiles['ENES2016.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2016-media_2.zip'
dicFiles['ENES2017.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2017-final.zip'
dicFiles['ENES2018.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2018.zip'
dicFiles['ENES2019.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2019.zip'

dicFiles['ENES2020.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes_2020.zip'
dicFiles['ENES2021.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2021.zip'
dicFiles['ENES2022.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/enes2022_bd_imprensa.zip'


dicFiles['ENEB2008.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2008-itens_1.zip'
dicFiles['ENEB2009.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2009.zip'
dicFiles['ENEB2010.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2010_eneb.zip'

dicFiles['ENEB2011.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2011_eneb.zip'
dicFiles['ENEB2012.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2012_eneb_eb.zip'
dicFiles['ENEB2013.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/2013_eneb_2_3ciclos.zip'
dicFiles['ENEB2014.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2014.zip'
dicFiles['ENEB2015.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2015.zip'
dicFiles['ENEB2016.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2016.zip'
dicFiles['ENEB2017.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2017-final.zip'
dicFiles['ENEB2018.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2018_final_1.zip'
dicFiles['ENEB2019.zip'] =  'http://www.dge.mec.pt/sites/default/files/JNE/eneb2019_0.zip'





# Specify the file path where you want to save the YAML data
path_dict_params = 'c:\ENES\Params.yaml'
path_dict_files = 'c:\ENES\Files.yaml'

# Save the dictionaries to a single YAML file
#with open(path_dict_params, 'w') as file:
#    yaml.dump_all([dicParams], file)
#with open(path_dict_files, 'w') as file:
#    yaml.dump_all([dicFiles], file)


dicParams = {}
with open(path_dict_params, 'r') as file:
    dicParams = yaml.load(file, Loader=yaml.FullLoader)


dicFiles = {}
with open(path_dict_files, 'r') as file:
    dicFiles = yaml.load(file, Loader=yaml.FullLoader)

