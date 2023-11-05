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

import traceback

#endregion


# region download zips and extract MDBs
def download_zip_files():
	# download files in dicFiles from the Internet
	from main import dicParams, dicFiles
	for key in dicFiles.keys():
		if not exists(dicParams['dataFolderZIP'] + key):
			print("Downloading " + key)
			wget.download(dicFiles[key], dicParams['dataFolderZIP'] + key)


def extract_MDBs():
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
# endregion download zips and extract MDBs


# region database and file functions
def mdbConnect( year=None, mdbfile=None):
	'''
	year: year of the database to connect to
	mdbfile: full or parcial path to the mdb file to connect to
	'''
	from main import dicParams
	if mdbfile is None and year is None:
		raise Exception("mdbConnect: Either year or mdbfile must be provided")
	if mdbfile is None:
		mdbfile = dicParams['dataFolderMDB'] +  'ENES' + str(year) + '.mdb'
	else:
		if mdbfile[-4:] != ".mdb":
			mdbfile = mdbfile + ".mdb"
		if mdbfile[0] != "/" & mdbfile[1] != ":":
			mdbfile = dicParams['dataFolderMDB'] + mdbfile
	connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
	connection = pyodbc.connect(connection_string)
	return connection

def read_sql_with_fallback( year, SQLcmds = []):
	success = False
	if type(SQLcmds) == str:
		SQLcmds = [SQLcmds]
	while len(SQLcmds) > 0 and not success:
		SQLcommand = SQLcmds.pop( 0)
		try:
			df = pd.read_sql(SQLcommand, mdbConnect(year))
			vprint("Successfully executed: ", SQLcommand)
			success = True
		except Exception as e:
			vprint(f"--> Failed to execute. Exception: {e}")
	if not success:
		raise Exception("All strings failed!")

	return df

def read_parquet( filename):
	from main import dicParams
	parquetPath = dicParams['dataFolderParquet']
	dfParquet = pd.read_parquet(dicParams['dataFolderParquet'] + filename + '.parquet.gzip')
	vprint("Loaded Parquet file ", filename, "shape: ", dfParquet.shape)
	return dfParquet

def write_parquet( df, filename):
	from main import dicParams
	parquetPath = dicParams['dataFolderParquet']
	df.to_parquet(parquetPath + filename + '.parquet.gzip', compression='gzip')
	vprint("Saved Parquet file ", filename, ", shape: ", df.shape)
# endregion database functions

# region Parquet Imports

def get_dfGeo_from_Parquet():
	'''Return dataframe with Distrito, Concelho and Nut3 information. Obtain data from parquet files.'''
	return read_parquet('dfGeo')

def get_dfSitFreq_from_Parquet():
	'''Return dataframe with Student enrollment (Situacao de Frequencia) information. Obtain data from parquet files.'''
	return read_parquet('dfSitFreq')

def get_dfSchools_from_Parquet():
	'''Return dataframe with School information. Obtain data from parquet files.'''
	return read_parquet("dfSchools")

def get_dfCursos_from_Parquet():
	'''Return dataframe with Course information. Obtain data from parquet files.'''
	return read_parquet("dfCursos")

def get_dfExames_from_Parquet():
	'''Return dataframe with Exam information. Obtain data from parquet files.'''
	return read_parquet("dfExames")

def get_dfResultados_from_Parquet():
	'''Return dataframe with Exam Results information. Obtain data from parquet files.'''
	return read_parquet("dfResultados")

def get_dfAll_from_Parquet():
	'''Return dataframe with all data joined, from parquet files.'''
	return read_parquet('dfAll')

def get_dfAllFase1_from_Parquet():
	'''Return dataframe with all data joined, from parquet files, just for Phase 1 exams.'''
	return read_parquet("dfAllFase1")

def get_dfInfoEscolas_from_Parquet():
	return read_parquet("dfInfoEscolas")

#endregion

# region MDB Imports

def get_dfGeo_from_MDB():
	'''
	Return dataframe with Distrito, Concelho and Nut3 information. Obtain data from MDB files.
	'''
	from main import dicParams, dicNuts2

	# Nuts3 description is in the 2018 database (defined as geoNuts3InfoYear); the other attributes are obtained from the 2021 database (defined as geoInfoYear)

	SQL = 'SELECT Nuts3, Descr as DescrNuts3 FROM tblNuts3;'
	dfNuts3 = read_sql_with_fallback( dicParams["geoNuts3infoYear"], SQLcmds = SQL)

	#Define Nuts2 as the first two characters in Nuts3
	dfNuts3["Nuts2"] = dfNuts3["Nuts3"].str[0:2]

	# Add Nuts2 description to the dataframe, using the dicNuts2 dictionary in novaenes.yaml
	dfNuts3["DescrNuts2"] = dfNuts3["Nuts2"].map(dicNuts2)

	SQL = 'SELECT conc.Distrito, distr.Descr as DescrDistrito, conc.Concelho, conc.Descr as DescrConcelho, conc.Nuts3 FROM tblCodsConcelho conc inner join tblCodsDistrito distr on conc.distrito = distr.distrito;'
	dfGeo = read_sql_with_fallback( dicParams["geoInfoYear"], SQLcmds = SQL)

	dfGeo = dfGeo.merge(dfNuts3, on="Nuts3", how="left")

	del dfNuts3

	write_parquet( dfGeo, 'dfGeo')

	return dfGeo


def get_dfSitFreq_from_MDB():
	''' Return dataframe with Student enrollment (Situacao de Frequencia) information. Obtain data from MDB files. '''
	from main import dicParams
	# SitFreq

	# LOAD SitFreq from 2018 (year defined in novaenes.yaml as sitFreqInfoYear)
	SQL = "SELECT SitFreq, Descr as SitFreqDescr, Defin as SitFreqDefin FROM tblCodsSitFreq ;"
	dfSitFreq = read_sql_with_fallback( dicParams["sitFreqInfoYear"], SQLcmds = SQL)

	write_parquet( dfSitFreq, 'dfSitFreq')
	return dfSitFreq


def get_dfSchools_from_MDB():
	''' Return dataframe with School information. Obtain data from MDB files.'''
	from main import dicParams

	paramFirstYear = dicParams['firstYear']
	paramLastYear = dicParams['lastYear']

	SQL = "SELECT " + str(paramLastYear) + " as AnoDadosEscola, Distrito, Concelho, Escola, Descr as DescrEscola, PubPriv, CodDGEEC FROM tblEscolas;"
	dfSchools = read_sql_with_fallback( paramLastYear, SQLcmds = SQL)

	print("Year {} - adding {} schools to the dataframe.".format( paramLastYear, dfSchools.shape[0]))

	# Now loop throught the previous years, in descending order, to get schools that no longer exist
	for ano in range( paramLastYear-1, paramFirstYear-1, -1):
		
		try:
			# Establish a connection to the database
			SQL1 = "SELECT " + str(ano) + " as AnoDadosEscola, Distrito, Concelho, Escola, Descr, PubPriv, CodDGEEC FROM tblEscolas;"
			SQL2 = "SELECT " + str(ano) + " as AnoDadosEscola, Distrito, Concelho, Escola, Descr, PubPriv, int( distrito * 100000 + concelho * 1000 + 999) as CodDGEEC FROM tblEscolas;"
			dfUpdates = read_sql_with_fallback(year=ano, SQLcmds=[SQL1, SQL2])

			# Identify rows in dfUpdates that don't exist in dfSchools, select them and then concatenate them to dfSchools
			mask = ~dfUpdates[['Escola']].apply(tuple, axis=1).isin(dfSchools[['Escola']].apply(tuple, axis=1))
			dfUpdates = dfUpdates[mask]
			dfUpdates['CodDGEEC'] = dfUpdates['CodDGEEC'].astype(int)
			dfSchools = pd.concat([dfSchools, dfUpdates])
			print("Year {} - adding an additional {} schools do dataframe, totaling {}.".format( ano, dfUpdates.shape[0], dfSchools.shape[0]))

			# Reset the index in the result DataFrame
			dfSchools.reset_index(drop=True, inplace=True)

		except Exception as ex:
			print("Ano:",ano," - Erro: ", ex.message)

	# Assuming 'CodDGEEC' is an integer column
	dfSchools['CodDGEEC'] = dfSchools['CodDGEEC'].astype(str)

	write_parquet( dfSchools, 'dfSchools')
	return dfSchools

def get_dfCursos_from_MDB():
	'''Return dataframe with Course information. Obtain data from MDB files.'''
	from main import dicParams, dicExamShortNames

	paramFirstYear = dicParams['firstYear']
	paramLastYear = dicParams['lastYear']

	for ano in range(paramLastYear, paramFirstYear-1, -1):
		try:
			# Execute SQL query
			SQL = "Select " + str(ano) + " as ano, curso.Curso, curso.TpCurso as TipoCurso, curso.SubTipo as SubtipoCurso, curso.Descr as DescrCurso,"\
			"tpcurso.Descr as DescrTipoCurso, tpcurso.Ano_Ini as TipoCurso_Ano_Ini, tpcurso.Ano_Term as TipoCurso_Ano_Term, tpcurso.Ordena as TipoCurso_Ordena,"\
			"subtipos.Descr as DescrSubtipoCurso "\
			"From ( tblCursos curso inner join tblCursosTipos tpcurso on curso.TpCurso = tpcurso.TpCurso ) inner join tblCursosSubTipos subtipos on curso.SubTipo = subtipos.SubTipo;"

			dfCursosAno = read_sql_with_fallback( year=ano, SQLcmds = SQL)
			print("Ano {} - cursos: {}".format(ano, dfCursosAno.shape[0]))

			if "dfCursos" not in locals():
				dfCursos = dfCursosAno
			else:
				dfCursos = dfCursos.append(dfCursosAno)
		except:
			print("get_dfCursos_from_MDB, Ano {} - ERRO!".format(ano))

	write_parquet( dfCursos, 'dfCursos')
	return( dfCursos)

def get_dfExames_from_MDB():
	'''Return dataframe with Exam information. Obtain data from MDB files.'''
	from main import dicParams, dicExamShortNames
	# Exames
	paramFirstYear = dicParams['firstYear']
	paramLastYear = dicParams['lastYear']

	for ano in range(paramLastYear, paramFirstYear-1, -1):
		try:
			# Execute SQL query
			SQL1 = "Select " + str(ano) + " as ano, Exame, Descr as DescrExame, TipoExame from tblExames;"
			SQL2 =  "Select " + str(ano) + " as ano, Exame, Descr as DescrExame, 'ND' as TipoExame from tblExames;"
			dfExamesAno = read_sql_with_fallback( year=ano, SQLcmds = [SQL1, SQL2])

			if "dfExames" not in locals():
				dfExames = dfExamesAno
			else:
				dfExames = dfExames.append(dfExamesAno)
		except:
			print("get_dfExames_from_MDB, Ano {} - ERRO!".format(ano))
	
		dfExames['DescrExameAbrev'] = dfExames['DescrExame'].replace(dicExamShortNames)

	write_parquet(dfExames, "dfExames")
	return dfExames

def get_dfResultados_from_MDB():
	'''Return dataframe with Exam Results information. Obtain data from MDB files.'''
	from main import dicParams
	paramFirstYear = dicParams['firstYear']
	paramLastYear = dicParams['lastYear']

	for ano in range(paramLastYear, paramFirstYear-1, -1):
		SQL1 = "Select " + str(ano) + " as ano, * from tblHomologa_" + str(ano) + " where class_exam between 0 and 200;"
		SQL2 = "Select " + str(ano) + " as ano, *, 'ND' as ParaCFCEPE from tblHomologa_" + str(ano) + " where class_exam between 0 and 200;"
		dfResultadosAno = read_sql_with_fallback( year=ano, SQLcmds = [SQL1, SQL2])

		dfResultadosAno['Class_Exam'] = dfResultadosAno['Class_Exam'] / 10
		dfResultadosAno['Sexo'] = dfResultadosAno['Sexo'].str.upper()
		dfResultadosAno['Class_Exam_Rounded'] = (dfResultadosAno['Class_Exam'] + 0.001).round().astype(int)
		dfResultadosAno['Class_Exam_RoundUp'] = (dfResultadosAno['Class_Exam'] + 0.49).round().astype(int)
		dfResultadosAno = dfResultadosAno.dropna(subset=['Class_Exam'])

		#use a lambda function to define new column "Covid" with value "Before" if ano < 2020, else "After"
		dfResultadosAno["Covid"] = dfResultadosAno.apply(lambda row: "Before" if row["ano"] < 2020 else "After", axis=1)

		if "dfResultados" not in locals():
			dfResultados = dfResultadosAno
		else:
			dfResultados = dfResultados.append(dfResultadosAno)

	write_parquet( dfResultados, 'dfResultados')
	return( dfResultados)

# endregion

# region dfAll from Datasets
def get_dfAll_from_datasets(dfGeo, dfSchools, dfCursos, dfExames, dfResultados, dfSitFreq):

	dfSchools = dfSchools.merge(dfGeo, left_on=['Distrito', 'Concelho'], right_on=['Distrito', 'Concelho'], how='inner')
	dfAll = dfResultados.merge(dfSchools, left_on=['Escola'], right_on=['Escola'], how='inner')

	dfAll = dfAll.merge(dfExames, left_on=['ano', 'Exame'], right_on=['ano', 'Exame'], how='inner')

	dfAll = dfAll.merge(dfSitFreq, left_on=['SitFreq'], right_on=['SitFreq'], how='left')
	dfAll["SitFreq"].fillna("ND", inplace=True)
	dfAll["SitFreqDescr"].fillna("ND", inplace=True)
	dfAll["SitFreqDefin"].fillna("ND", inplace=True)

	# We're eliminating 83 results whose "curso" is not in the list of courses for that year. 2009: 14; 2010: 47; 2011: 22
	dfAll = dfAll.merge(dfCursos, left_on=['ano', 'Curso'], right_on=['ano', 'Curso'], how='inner')
	
	write_parquet( dfAll, 'dfAll')
	return( dfAll)

def get_dfAllFase1_from_datasets(dfAll):
	# We're eliminating 83 results whose "curso" is not in the list of courses for that year. 2009: 14; 2010: 47; 2011: 22
	dfAllFase1 = dfAll[dfAll["Fase"]=='1']
	
	write_parquet( dfAllFase1, 'dfAllFase1')
	return( dfAllFase1)

    
def get_dfInfoEscolas_from_datasets(dfAllFase1):

	myclock= vprint_time(prefix = "dfInfoEscolas - Applying ""InfoEscolas"" on dfAllFase1 filters and calculating group statistcs.")
	# We're eliminating 83 results whose "curso" is not in the list of courses for that year. 2009: 14; 2010: 47; 2011: 22
	dfInfoEscolas = dfAllFase1[ (dfAllFase1["TemInterno"] == "S") & (dfAllFase1["SubtipoCurso"].isin(["N01", "N04"])) & (dfAllFase1["Fase"] == "1") & (dfAllFase1["ParaAprov"] == "S") & (dfAllFase1["Interno"] == "S") & (dfAllFase1["Class_Exam"] > 9.4)]
	
	# Class_Exam_Rounded
	dfStats = dfInfoEscolas.groupby(['ano', "Exame", 'Class_Exam_Rounded']).agg(['median', 'mean', 'count'])['CIF'].rename(columns={'median': 'ExamRounded_median_CIF', 'mean': 'ExamRounded_mean_CIF', 'count': 'ExamRounded_count_CIF'})
	dfInfoEscolas = dfInfoEscolas.merge(dfStats, left_on=['ano', "Exame", 'Class_Exam_Rounded'], right_on=['ano', "Exame", 'Class_Exam_Rounded'], how='left')

	# Class_Exam_Roundup
	dfStats = dfInfoEscolas.groupby(['ano', "Exame", 'Class_Exam_RoundUp']).agg(['median', 'mean', 'count'])['CIF'].rename(columns={'median': 'ExamRoundUp_median_CIF', 'mean': 'ExamRoundUp_mean_CIF', 'count': 'ExamRoundUp_count_CIF'})
	dfInfoEscolas = dfInfoEscolas.merge(dfStats, left_on=['ano', "Exame", 'Class_Exam_RoundUp'], right_on=['ano', "Exame", 'Class_Exam_RoundUp'], how='left')

	# Bonus is the actual grade (CIF) minus the expected grade. Positive if actual CIF was above expected; negative if it was below expected
	dfInfoEscolas["CIF_bonus_median_ExamRounded"] = dfInfoEscolas["CIF"] - dfInfoEscolas["ExamRounded_median_CIF"]
	dfInfoEscolas["CIF_bonus_mean_ExamRounded"]   = dfInfoEscolas["CIF"] - dfInfoEscolas["ExamRounded_mean_CIF"]
	dfInfoEscolas["CIF_bonus_median_ExamRoundUp"] = dfInfoEscolas["CIF"] - dfInfoEscolas["ExamRoundUp_median_CIF"]
	dfInfoEscolas["CIF_bonus_mean_ExamRoundUp"]   = dfInfoEscolas["CIF"] - dfInfoEscolas["ExamRoundUp_mean_CIF"]

	# Adjusted Bonus is the same, but we compensate for the rounding done in Class_Exam_Rounded or Class_Exam_RoundUp. If we rounded up, we compensate by subtracting that, and vice-versa
	dfInfoEscolas["CIF_bonus_adj_median_ExamRounded"] = dfInfoEscolas["CIF_bonus_median_ExamRounded"] + dfInfoEscolas["Class_Exam"] - dfInfoEscolas["Class_Exam_Rounded"]
	dfInfoEscolas["CIF_bonus_adj_mean_ExamRounded"]   = dfInfoEscolas["CIF_bonus_mean_ExamRounded"] + dfInfoEscolas["Class_Exam"] - dfInfoEscolas["Class_Exam_Rounded"]
	dfInfoEscolas["CIF_bonus_adj_median_ExamRoundUp"] = dfInfoEscolas["CIF_bonus_median_ExamRoundUp"] + dfInfoEscolas["Class_Exam"] - dfInfoEscolas["Class_Exam_RoundUp"]
	dfInfoEscolas["CIF_bonus_adj_mean_ExamRoundUp"]   = dfInfoEscolas["CIF_bonus_mean_ExamRoundUp"] + dfInfoEscolas["Class_Exam"] - dfInfoEscolas["Class_Exam_RoundUp"]

	write_parquet( dfInfoEscolas, 'dfInfoEscolas')
	myclock= vprint_time(prefix = "dfInfoEscolas - created and save to parquet! ", start_time=myclock)
	return( dfInfoEscolas)
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
		vprint(prefix + ' Current time: ' + str(datetime.datetime.now()))
	else:
		elapsed_time = current_time - start_time
		vprint(prefix + ' Current time: ' + str(datetime.datetime.now()) + '; Elapsed time: ' + str(datetime.timedelta(seconds=elapsed_time)))
	return current_time
# endregion
