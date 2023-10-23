#!pip install wget

if dicParams["doDownloadZips"] == True:
    # loading the temp.zip and creating a zip object
	firstrun_download_files()

if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True:
    # loading the temp.zip and creating a zip object
	firstrun_extract_MDBs()
     

if dicParams["doDownloadZips"] == True or dicParams["doExtractMDB"] == True or dicParams["doCreateDatasets"] == True:

	

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

	dfGeo








