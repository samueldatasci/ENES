# ImportUtils

#from main import dicParams, dicFiles
from shared import *
import pyodbc
import pandas as pd





def firstrun_download_files():
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
            
			


def get_dfGeo_from_Parquet():
	'''Return dataframe with Distrito, Concelho and Nut3 information. Obtain data from parquet files.
	'''
	parquetPath = dicParams['dataFolderParquet']
	dfGeo = pd.read_parquet(parquetPath + 'dfGeo.parquet.gzip')  
	return dfGeo

def get_dfSitFreq_from_Parquet():
	'''
	Return dataframe with Student enrollment (Situacao de Frequencia) information. Obtain data from parquet files.
	'''
	# SitFreq
	parquetPath = dicParams['dataFolderParquet']
	dfSitFreq = pd.read_parquet(parquetPath + 'dfSitFreq.parquet.gzip')  
	return dfSitFreq


def get_dfSchools_from_Parquet():
	'''Return dataframe with School information. Obtain data from parquet files.
	'''
	# Schools
	parquetPath = dicParams['dataFolderParquet']
	dfSchools = pd.read_parquet(parquetPath + 'dfSchools.parquet.gzip')  
	return dfSchools


def get_dfCursos_from_Parquet():
	'''Return dataframe with Course information. Obtain data from parquet files.'''
	# Curso, tipo, subtipo
	parquetPath = dicParams['dataFolderParquet']
	dfCursos = pd.read_parquet(parquetPath + 'dfCursos.parquet.gzip')  
	return dfCursos


def get_dfExames_from_Parquet():
	'''Return dataframe with Exam information. Obtain data from parquet files.'''
	# Exames
	parquetPath = dicParams['dataFolderParquet']
	dfExames = pd.read_parquet(parquetPath + 'dfExames.parquet.gzip')  
	return dfExames


def get_dfResultados_from_Parquet():
	'''Return dataframe with Exam Results information. Obtain data from parquet files.'''
	# Resultados dos exames
	parquetPath = dicParams['dataFolderParquet']
	dfResultados = pd.read_parquet(parquetPath + 'dfResultados.parquet.gzip')  
	return dfResultados



def get_dfResultAnalise_from_Parquet():
	'''Return dataframe with relevant subset of Exam Results information. Obtain data from parquet files.'''
	# Resultados dos Exames
	# Apenas Fase 1
	# Apenas com TemInterno = 1

	parquetPath = dicParams['dataFolderParquet']
	dfResultAnalise.to_parquet(parquetPath + 'dfResultAnalise.parquet.gzip', compression='gzip')  

	return(dfResultAnalise)







def get_dfGeo_from_MDB():
	'''
	Return dataframe with Distrito, Concelho and Nut3 information. Obtain data from MDB files.
	'''
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


	# Get Distrito+Concelho+Nuts from the 2021 database

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
	# Schools

	# LOAD 2022

	ano = 2022
	# Establish a connection to the database
	mdbfile = dicParams['dataFolderMDB'] +  'ENES' + str(ano) + '.mdb'
	connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
	connection = pyodbc.connect(connection_string)

	# Execute SQL query
	SQL = "SELECT 2022 as AnoDadosEscola, Distrito, Concelho, Escola, Descr, PubPriv, CodDGEEC FROM tblEscolas;"
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
			mask = ~dfUpdates[['Distrito', 'Concelho', 'Escola']].apply(tuple, axis=1).isin(dfSchools[['Distrito', 'Concelho', 'Escola']].apply(tuple, axis=1))

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
	#dfSchools['CodDGEEC'] = dfSchools['CodDGEEC'].astype(bytes)

	# Assuming 'CodDGEEC' is an integer column
	dfSchools['CodDGEEC'] = dfSchools['CodDGEEC'].astype(str)

	parquetPath = dicParams['dataFolderParquet']
	dfSchools.to_parquet(parquetPath + 'dfSchools.parquet.gzip', compression='gzip')  

	return dfSchools




def get_dfCursos_from_MDB():
	'''
	Return dataframe with Course information. Obtain data from MDB files.
	'''

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
			print("Ano {} - ERRO!".format(ano))

	# Close the connection
	connection.close()

	parquetPath = dicParams['dataFolderParquet']
	dfCursos.to_parquet(parquetPath + 'dfCursos.parquet.gzip', compression='gzip')  

	return( dfCursos)



def get_dfExames_from_MDB():
	'''
	Return dataframe with Exam information. Obtain data from MDB files.
	'''
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

			# print("Ano {} - exames: {}".format(ano, dfExamesAno.shape[0]))
			# print(dfExamesAno)

			if "dfExames" not in locals():
				dfExames = dfExamesAno
			else:
				dfExames = dfExames.append(dfExamesAno)
		except:
			print("Ano {} - ERRO!".format(ano))

	# Close the connection
	connection.close()

	parquetPath = dicParams['dataFolderParquet']
	dfExames.to_parquet(parquetPath + 'dfExames.parquet.gzip', compression='gzip')  

	return dfExames





def get_dfResultados_from_MDB():
	'''
	Return dataframe with Exam Results information. Obtain data from MDB files.
	'''
	# Resultados dos Exames


	# LOAD Resultados for all years

	for ano in range(2022, 2021-1, -1):
	#for ano in range(2022, 2008-1, -1):
		try:
			# Establish a connection to the database
			mdbfile = dicParams['dataFolderMDB'] +  'ENES'+str(ano)+'.mdb'
			connection_string = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + mdbfile
			connection = pyodbc.connect(connection_string)

			# Execute SQL query
			if ano > 2015:
				SQL = "Select " + str(ano) + " as ano, * from tblHomologa_" + str(ano) + ";"
			else:
				SQL = "Select " + str(ano) + " as ano, *, '?' as ParaCFCEPE from tblHomologa_" + str(ano) + ";"

			dfResultadosAno = pd.read_sql(SQL, connection)

			#df[ano] = dfResultadosAno

			if "dfResultados" not in locals():
				dfResultados = dfResultadosAno
			else:
				dfResultados = dfResultados.append(dfResultadosAno)
		except:
			print("Ano {} - ERRO!".format(ano))

	# Close the connection
	connection.close()
		
	parquetPath = dicParams['dataFolderParquet']
	dfResultados.to_parquet(parquetPath + 'dfResultados.parquet.gzip', compression='gzip')  

	return( dfResultados)






def get_dfResultAnalise_from_MDB():
	'''
	Return dataframe with relevant subset of Exam Results information. Obtain data from MDB files.
	'''
	# Resultados dos Exames
	# Apenas Fase 1
	# Apenas com TemInterno = 1

	dfResultAnalise = get_dfResultados_from_MDB()

	dfResultAnalise = dfResultAnalise[dfResultAnalise["Fase"]=='1']
	dfResultAnalise = dfResultAnalise[dfResultAnalise["TemInterno"]=='S']

	# Reset the index if needed
	dfResultAnalise.reset_index(drop=True, inplace=True)


	parquetPath = dicParams['dataFolderParquet']
	dfResultAnalise.to_parquet(parquetPath + 'dfResultAnalise.parquet.gzip', compression='gzip')  

	return( dfResultAnalise)



x = get_dfResultados_from_MDB()

print("Resultados: ", x.shape)
print("Done...")
