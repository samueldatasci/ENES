# ImportUtils

# region imports
import pandas as pd
import numpy as np
import yaml
import pyodbc

from os import rename
from os.path import exists
import time, datetime

# importing the zipfile module
from zipfile import ZipFile
  
# importing the requests module
import requests
import wget

#from sqlalchemy import create_engine
import traceback

#endregion


def firstrun_download_files():
	# download files in dicFiles from the Internet
	from main import dicParams, dicFiles
	for key in dicFiles.keys():
		if not exists(dicParams['dataFolderZIP'] + key):
			print("Downloading " + key)
			wget.download(dicFiles[key], dicParams['dataFolderZIP'] + key)


def firstrun_extract_MDBs():
	from main import dicParams, dicFiles
	for key in dicFiles.keys():
		zipfile = dicParams['dataFolderZIP'] + key

		with ZipFile(zipfile, 'r') as zObject:
			path=dicParams['dataFolderMDB'] + key[:-4] + ".mdb"
			filename = ZipFile.namelist(zObject)[0]
			# Extracting specific file in the zip into a specific location.
			zObject.extract( member=filename, path=dicParams['dataFolderMDB'])
			print("Rename {} to {}".format(dicParams['dataFolderMDB'] +filename, dicParams['dataFolderMDB'] + key[:-4] + ".mdb"))
			rename(dicParams['dataFolderMDB'] + filename, dicParams['dataFolderMDB'] + key[:-4] + ".mdb")


# region Parquet Imports

def get_dfGeo_from_Parquet():
	'''Return dataframe with Distrito, Concelho and Nut3 information. Obtain data from parquet files.
	'''
	from main import dicParams, dicFiles
	parquetPath = dicParams['dataFolderParquet']
	dfGeo = pd.read_parquet(parquetPath + 'dfGeo.parquet.gzip')  
	return dfGeo

def get_dfSitFreq_from_Parquet():
	'''
	Return dataframe with Student enrollment (Situacao de Frequencia) information. Obtain data from parquet files.
	'''
	from main import dicParams, dicFiles
	# SitFreq
	parquetPath = dicParams['dataFolderParquet']
	dfSitFreq = pd.read_parquet(parquetPath + 'dfSitFreq.parquet.gzip')  
	return dfSitFreq

def get_dfSchools_from_Parquet():
	'''Return dataframe with School information. Obtain data from parquet files.
	'''
	from main import dicParams, dicFiles
	# Schools
	parquetPath = dicParams['dataFolderParquet']
	dfSchools = pd.read_parquet(parquetPath + 'dfSchools.parquet.gzip')  
	return dfSchools

def get_dfCursos_from_Parquet():
	'''Return dataframe with Course information. Obtain data from parquet files.'''
	from main import dicParams, dicFiles
	# Curso, tipo, subtipo
	parquetPath = dicParams['dataFolderParquet']
	dfCursos = pd.read_parquet(parquetPath + 'dfCursos.parquet.gzip')  
	return dfCursos

def get_dfExames_from_Parquet():
	'''Return dataframe with Exam information. Obtain data from parquet files.'''
	from main import dicParams, dicFiles
	# Exames
	parquetPath = dicParams['dataFolderParquet']
	dfExames = pd.read_parquet(parquetPath + 'dfExames.parquet.gzip')  
	return dfExames

def get_dfResultados_from_Parquet():
	'''Return dataframe with Exam Results information. Obtain data from parquet files.'''
	from main import dicParams, dicFiles
	# Resultados dos exames
	parquetPath = dicParams['dataFolderParquet']
	dfResultados = pd.read_parquet(parquetPath + 'dfResultados.parquet.gzip')
	
	# print record count by ano
	#print(dfResultados.groupby('ano').count())

	return dfResultados

def get_dfResultAnalise_from_Parquet():
	'''Return dataframe with relevant subset of Exam Results information. Obtain data from parquet files.'''
	from main import dicParams, dicFiles
	# Resultados dos Exames
	# Apenas Fase 1
	# Apenas com TemInterno = 1

	parquetPath = dicParams['dataFolderParquet']
	dfResultAnalise = pd.read_parquet(parquetPath + 'dfResultAnalise.parquet.gzip')

	# print record count by ano
	#print(dfResultAnalise.groupby('ano').count())

	return(dfResultAnalise)

def get_dfAll_from_Parquet():
	'''Return dataframe with all data joined, from parquet files.'''
	from main import dicParams, dicFiles

	parquetPath = dicParams['dataFolderParquet']
	dfAll = pd.read_parquet(parquetPath + 'dfAll.parquet.gzip')

	return( dfAll)

# endregion


# region MDB Imports

def get_dfGeo_from_MDB():
	'''
	Return dataframe with Distrito, Concelho and Nut3 information. Obtain data from MDB files.
	'''
	from main import dicParams, dicFiles
	# Distrito+Concelho+Nuts3

	# Nuts3 description is in the 2018 database; the other attributes are obtained from the 2021 database
	#dfLocation = pd.DataFrame( {'Distrito':[], 'DescrDistrito':[], 'Concelho': [], 'DescrConcelho': [], 'Nuts3': [], 'Nuts3Descr': []})

	# Get Nuts3 names from 2018 database
	# Establish a connection to the database
	mdbfile = dicParams['dataFolderMDB'] +  'ENES2018.mdb'
	connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
	connection = pyodbc.connect(connection_string)

	# Execute SQL query
	SQL = 'SELECT Nuts3, Descr as DescrNuts3 FROM tblNuts3;'
	dfNuts3 = pd.read_sql(SQL, connection)

	#Define Nuts2 as the first two characters in Nuts3
	dfNuts3["Nuts2"] = dfNuts3["Nuts3"].str[0:2]

	print(dfNuts3)
	
	# Define dictionary for Nuts2 description
	dicNuts2 = {}
	dicNuts2["11"] = "Norte"
	dicNuts2["15"] = "Algarve"
	dicNuts2["16"] = "Centro"
	dicNuts2["17"] = "AM Lisboa"
	dicNuts2["18"] = "Alentejo"
	dicNuts2["20"] = "RA Açores"
	dicNuts2["30"] = "RA Madeira"
	dicNuts2["90"] = "Estrangeiro"

	# Add Nuts2 description to the dataframe
	dfNuts3["DescrNuts2"] = dfNuts3["Nuts2"].map(dicNuts2)

	# Establish a connection to the database
	mdbfile = dicParams['dataFolderMDB'] +  'ENES2021.mdb'
	connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
	connection = pyodbc.connect(connection_string)

	# Execute SQL query
	SQL = 'SELECT conc.Distrito, distr.Descr as DescrDistrito, conc.Concelho, conc.Descr as DescrConcelho, conc.Nuts3 FROM tblCodsConcelho conc inner join tblCodsDistrito distr on conc.distrito = distr.distrito;'

	dfGeo = pd.read_sql(SQL, connection)

	# Close the cursor and connection
	connection.close()

	dfGeo = dfGeo.merge(dfNuts3, on="Nuts3", how="left")

	del dfNuts3

	parquetPath = dicParams['dataFolderParquet']
	dfGeo.to_parquet(parquetPath + 'dfGeo.parquet.gzip', compression='gzip')  

	return dfGeo


def get_dfSitFreq_from_MDB():
	'''
	Return dataframe with Student enrollment (Situacao de Frequencia) information. Obtain data from MDB files.
	'''
	from main import dicParams, dicFiles
	# SitFreq

	# LOAD SitFreq from 2018

	# Establish a connection to the database
	mdbfile = dicParams['dataFolderMDB'] +  'ENES2018.mdb'
	connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
	connection = pyodbc.connect(connection_string)

	# Execute SQL query
	SQL = "SELECT SitFreq, Descr, Defin FROM tblCodsSitFreq;"
	dfSitFreq = pd.read_sql(SQL, connection)

	# Close the connection
	connection.close()

	parquetPath = dicParams['dataFolderParquet']
	dfSitFreq.to_parquet(parquetPath + 'dfSitFreq.parquet.gzip', compression='gzip')  

	return dfSitFreq

def get_dfSchools_from_MDB():
	'''
	Return dataframe with School information. Obtain data from MDB files.
	'''
	from main import dicParams, dicFiles
	# Schools

	# LOAD 2022

	ano = 2022
	# Establish a connection to the database
	mdbfile = dicParams['dataFolderMDB'] +  'ENES' + str(ano) + '.mdb'
	connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
	connection = pyodbc.connect(connection_string)

	# Execute SQL query
	SQL = "SELECT 2022 as AnoDadosEscola, Distrito, Concelho, Escola, Descr as DescrEscola, PubPriv, CodDGEEC FROM tblEscolas;"
	dfSchools = pd.read_sql(SQL, connection)

	# Close the connection
	connection.close()
	print("Year {} - adding {} schools to the dataframe.".format( ano, dfSchools.shape[0]))

	# Now loop throught the previous years, in descending order, to get schools that no longer exist
	for ano in range( 2022-1, 2008-1, -1):
		
		try:
			# Establish a connection to the database
			mdbfile = dicParams['dataFolderMDB'] +  'ENES' + str(ano) + '.mdb'
			connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
			connection = pyodbc.connect(connection_string)

			# Execute SQL query
			if ano >= 2015:
				SQL = "SELECT " + str(ano) + " as AnoDadosEscola, Distrito, Concelho, Escola, Descr, PubPriv, CodDGEEC FROM tblEscolas;"
			else:
				SQL = "SELECT " + str(ano) + " as AnoDadosEscola, Distrito, Concelho, Escola, Descr, PubPriv, int( distrito * 100000 + concelho * 1000 + 999) as CodDGEEC FROM tblEscolas;"
			
			dfUpdates = pd.read_sql(SQL, connection)

			# Identify rows in dfUpdates that don't exist in dfSchools
			#mask = ~dfUpdates[['Distrito', 'Concelho', 'Escola']].apply(tuple, axis=1).isin(dfSchools[['Distrito', 'Concelho', 'Escola']].apply(tuple, axis=1))
			mask = ~dfUpdates[['Escola']].apply(tuple, axis=1).isin(dfSchools[['Escola']].apply(tuple, axis=1))

			# Filter rows in dfUpdates based on the mask
			dfUpdates = dfUpdates[mask]
			dfUpdates['CodDGEEC'] = dfUpdates['CodDGEEC'].astype(int)

			# Concatenate dfSchools and the filtered new_rows
			dfSchools = pd.concat([dfSchools, dfUpdates])

			print("Year {} - adding an additional {} schools do dataframe, totaling {}.".format( ano, dfUpdates.shape[0], dfSchools.shape[0]))

			# Reset the index in the result DataFrame
			dfSchools.reset_index(drop=True, inplace=True)

			# Close the connection
			connection.close()
		except Exception as ex:
			print("Ano:",ano," - Erro: ", ex.message)

	# Assuming 'CodDGEEC' is an integer column
	dfSchools['CodDGEEC'] = dfSchools['CodDGEEC'].astype(str)

	parquetPath = dicParams['dataFolderParquet']
	dfSchools.to_parquet(parquetPath + 'dfSchools.parquet.gzip', compression='gzip')  

	return dfSchools

def get_dfCursos_from_MDB():
	'''
	Return dataframe with Course information. Obtain data from MDB files.
	'''

	from main import dicParams, dicFiles
	# Curso, tipo, subtipo

	for ano in range(2022, 2008-1, -1):
		try:
			# Establish a connection to the database
			mdbfile = dicParams['dataFolderMDB'] +  'ENES'+str(ano)+'.mdb'
			connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
			connection = pyodbc.connect(connection_string)

			# Execute SQL query
			SQL = "Select " + str(ano) + " as ano, curso.Curso, curso.TpCurso as TipoCurso, curso.SubTipo as SubtipoCurso, curso.Descr as DescrCurso,"\
			"tpcurso.Descr as DescrTipoCurso, tpcurso.Ano_Ini as TipoCurso_Ano_Ini, tpcurso.Ano_Term as TipoCirso_Ano_Term, tpcurso.Ordena as TipoCurso_Ordena,"\
			"subtipos.Descr as DescrSubtipoCurso "\
			"From ( tblCursos curso inner join tblCursosTipos tpcurso on curso.TpCurso = tpcurso.TpCurso ) inner join tblCursosSubTipos subtipos on curso.SubTipo = subtipos.SubTipo;"

			dfCursosAno = pd.read_sql(SQL, connection)

			print("Ano {} - cursos: {}".format(ano, dfCursosAno.shape[0]))

			if "dfCursos" not in locals():
				dfCursos = dfCursosAno
			else:
				dfCursos = dfCursos.append(dfCursosAno)
		except:
			print("get_dfCursos_from_MDB, Ano {} - ERRO!".format(ano))

	# Close the connection
	connection.close()

	parquetPath = dicParams['dataFolderParquet']
	dfCursos.to_parquet(parquetPath + 'dfCursos.parquet.gzip', compression='gzip')  

	return( dfCursos)

def get_dfExames_from_MDB():
	'''
	Return dataframe with Exam information. Obtain data from MDB files.
	'''
	from main import dicParams, dicFiles
	# Exames

	for ano in range(2022, 2008-1, -1):
		try:
			# Establish a connection to the database
			mdbfile = dicParams['dataFolderMDB'] +  'ENES'+str(ano)+'.mdb'
			connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
			connection = pyodbc.connect(connection_string)

			# Execute SQL query
			if ano > 2014:
				SQL = "Select " + str(ano) + " as ano, Exame, Descr as DescrExame, TipoExame from tblExames;"
			else:
				SQL = "Select " + str(ano) + " as ano, Exame, Descr as DescrExame, '?' as TipoExame from tblExames;"
			
			dfExamesAno = pd.read_sql(SQL, connection)

			if "dfExames" not in locals():
				dfExames = dfExamesAno
			else:
				dfExames = dfExames.append(dfExamesAno)
		except:
			print("get_dfExames_from_MDB, Ano {} - ERRO!".format(ano))

		replacement_dict = {}
		replacement_dict['Biologia e Geologia'] = 'Biologia/Geol.'
		replacement_dict['Física e Química A'] = 'Física/Quim. A'
		replacement_dict['Matemática Aplic. às Ciências Soc.'] = 'MACS'
		replacement_dict['Geometria Descritiva A'] = 'Geometr.D.A'
		replacement_dict['História da Cultura e das Artes'] = 'Hist.Cult.Artes'
		replacement_dict['Literatura Portuguesa'] = 'Liter.Portuguesa'
		replacement_dict['Espanhol (iniciação)'] = 'Espanhol (inic.)'
		dfExames['DescrExameAbrev'] = dfExames['DescrExame'].replace(replacement_dict)


	# Close the connection
	connection.close()

	parquetPath = dicParams['dataFolderParquet']
	dfExames.to_parquet(parquetPath + 'dfExames.parquet.gzip', compression='gzip')  

	return dfExames

def get_dfResultados_from_MDB():
	'''
	Return dataframe with Exam Results information. Obtain data from MDB files.
	'''
	from main import dicParams, dicFiles
	# Resultados dos Exames


	# LOAD Resultados for all years

	for ano in range(2022, 2008-1, -1):
		try:
			# Establish a connection to the database
			mdbfile = dicParams['dataFolderMDB'] +  'ENES'+str(ano)+'.mdb'
			connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
			connection = pyodbc.connect(connection_string)

			# Execute SQL query
			if ano > 2015:
				SQL = "Select " + str(ano) + " as ano, * from tblHomologa_" + str(ano) + " where class_exam between 0 and 200;"
			else:
				SQL = "Select " + str(ano) + " as ano, *, '?' as ParaCFCEPE from tblHomologa_" + str(ano) + " where class_exam between 0 and 200;"

			dfResultadosAno = pd.read_sql(SQL, connection)
			dfResultadosAno['Class_Exam'] = dfResultadosAno['Class_Exam'] / 10
			dfResultadosAno['Sexo'] = dfResultadosAno['Sexo'].str.upper()
			dfResultadosAno['Class_Exam_Rounded'] = (dfResultadosAno['Class_Exam'] + 0.001).round().astype(int)
			dfResultadosAno = dfResultadosAno.dropna(subset=['Class_Exam'])

			#use a lambda function to define new column "Covid" with value "Before" if ano < 2020, else "After"
			dfResultadosAno["Covid"] = dfResultadosAno.apply(lambda row: "Before" if row["ano"] < 2020 else "After", axis=1)

			if "dfResultados" not in locals():
				dfResultados = dfResultadosAno
			else:
				dfResultados = dfResultados.append(dfResultadosAno)
		except:
			print("get_dfResultados_from_MDB, Ano {} - ERRO!".format(ano))

	# Close the connection
	connection.close()

	parquetPath = dicParams['dataFolderParquet']
	dfResultados.to_parquet(parquetPath + 'dfResultados.parquet.gzip', compression='gzip')

	return( dfResultados)

def get_dfResultAnalise_from_MDB():
	'''
	Return dataframe with relevant subset of Exam Results information. Obtain data from MDB files.
	'''
	from main import dicParams, dicFiles
	# Resultados dos Exames
	# Apenas Fase 1
	# Apenas com TemInterno = 1

	dfResultAnalise = get_dfResultados_from_MDB()

	dfResultAnalise = dfResultAnalise[dfResultAnalise["Fase"]=='1']
	#dfResultAnalise = dfResultAnalise[dfResultAnalise["TemInterno"]=='S'] -- For 2020 and after, this is always 'N'

	# Reset the index if needed
	dfResultAnalise.reset_index(drop=True, inplace=True)


	parquetPath = dicParams['dataFolderParquet']
	dfResultAnalise.to_parquet(parquetPath + 'dfResultAnalise.parquet.gzip', compression='gzip')  

	return( dfResultAnalise)

# endregion

# region dfAll from Datasets
def get_dfAll_from_datasets(dfGeo, dfSchools, dfExames, dfResultados):
	from main import dicParams, dicFiles

	dfFullSchools = dfSchools.merge(dfGeo, left_on=['Distrito', 'Concelho'], right_on=['Distrito', 'Concelho'], how='inner')
	dfAll1 = dfResultados.merge(dfFullSchools, left_on=['Escola'], right_on=['Escola'], how='inner')
	dfAll = dfAll1.merge(dfExames, left_on=['ano', 'Exame'], right_on=['ano', 'Exame'], how='inner')

	parquetPath = dicParams['dataFolderParquet']
	dfAll.to_parquet(parquetPath + 'dfAll.parquet.gzip', compression='gzip')

	del dfFullSchools, dfAll1

	return( dfAll)

# endregion


# region print utilities


def vprint(*args, **kwargs):
	from main import dicParams
	if dicParams["verbose"] == True:
		print(*args, **kwargs)

# print current time and elapsed time since last execution
def vprint_time(start_time = 0, prefix = ''):
	current_time = time.time()
	if start_time == 0:
		vprint(prefix + 'Current time: ' + str(datetime.datetime.now()))
	else:
		elapsed_time = current_time - start_time
		vprint(prefix + 'Current time: ' + str(datetime.datetime.now()) + '; Elapsed time: ' + str(datetime.timedelta(seconds=elapsed_time)))
	return current_time

# endregion

