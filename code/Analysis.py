# region Imports

import pandas as pd
from main import dicParams, dicFiles
from ImportUtils import vprint, vprint_time
#from ImportUtils import get_dfGeo_from_MDB, get_dfSchools_from_MDB, get_dfCursos_from_MDB, get_dfExames_from_MDB, get_dfResultados_from_MDB, get_dfResultAnalise_from_MDB
from ImportUtils import get_dfGeo_from_Parquet, get_dfSchools_from_Parquet, get_dfCursos_from_Parquet, get_dfExames_from_Parquet, get_dfResultados_from_Parquet, get_dfResultAnalise_from_Parquet

import matplotlib.pyplot as plt

# Set the locale to your system's default (for the desired thousands separator)
import locale
locale.setlocale(locale.LC_ALL, 'pt_PT')

from main import dicParams, dicFiles
from ChartUtils import linechart, barchart, barchart_nseries
#endregion


# region Load data from Parquet files

# Load datasets from parquet files
vprint("Loading Parquet...")
current_time = vprint_time(0, 'Before load datasets...')

# region inactive dataframes
#dfGeo           = get_dfGeo_from_Parquet()
#vprint("dfGeo.shape: ", dfGeo.shape)
#current_time = vprint_time(current_time, 'Loaded dfGeo from Parquet...')
#dfSchools       = get_dfSchools_from_Parquet()
#vprint("dfSchools.shape: ", dfSchools.shape)
#current_time = vprint_time(current_time, 'Loaded dfSchools from Parquet...')
#dfCursos        = get_dfCursos_from_Parquet()
#vprint("dfCursos.shape: ", dfCursos.shape)
#current_time = vprint_time(current_time, 'Loaded dfCursos from Parquet...')
#dfExames        = get_dfExames_from_Parquet()
#vprint("dfExames.shape: ", dfExames.shape)
#current_time = vprint_time(current_time, 'Loaded dfExames from Parquet...')
# endregion

dfResultados    = get_dfResultados_from_Parquet()
vprint("dfResultados.shape: ", dfResultados.shape)
current_time = vprint_time(current_time, 'Loaded dfResultados from Parquet...')
#vprint("dfResultados.shape: ", dfResultados.shape)
#current_time = vprint_time(current_time, 'Loaded dfResultados from Parquet...')


dfResultAnalise = get_dfResultAnalise_from_Parquet()
vprint("dfResultAnalise.shape: ", dfResultAnalise.shape)
current_time = vprint_time(current_time, 'Loaded dfResultAnalise from Parquet...')


# endregion


dfExamesPorAno = dfResultados.groupby(['ano', 'Fase']).size().reset_index(name='counts')
# PUBPRIV dfExamesPorSexo = dfResultados.groupby(['ano', 'Sexo']).size().reset_index(name='counts')
dfExamesPorSexo = dfResultados.groupby(['ano', 'Sexo']).size().reset_index(name='counts')
#print(dfExamesPorAno.head(10))
#print(dfResultados.head(10))

#barchart_nseries(dfExamesPorAno, index='ano', columns='Fase', values='counts')
barchart_nseries(dfResultados, index='ano', columns='Fase', values='counts')
barchart_nseries(dfResultados, index='ano', columns='Sexo', values='counts')
# barchart_nseries(dfResultados, index='ano', columns='Fase', values='count',  title=None, xlabel='Ano', ylabel=None, grid=True, stacked=False, colormap='Set3')

#barchart(dfExamesPorAno, "ano", "counts", "Fase", "Número de exames", "Ano", "Distribuição de exames por ano e fase", True)



# region Create charts comparing All Years, 2019 and 2020
# dfResultAnalise = dfResultados
# dfResultAnaliseAll = dfResultAnalise
# linechart(dfResultAnaliseAll, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )
# dfResultAnaliseAll = dfResultAnalise[(dfResultAnalise['Fase'] == '1')]
# linechart(dfResultAnaliseAll, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )
# dfResultAnaliseAll = dfResultAnalise[(dfResultAnalise['Fase'] == '2')]
# linechart(dfResultAnaliseAll, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )

# dfResultAnalise2019 = dfResultAnalise[(dfResultAnalise['ano'] == 2019)]
# linechart(dfResultAnalise2019, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )
# dfResultAnalise2019 = dfResultAnalise[(dfResultAnalise['ano'] == 2019) & (dfResultAnalise['Fase'] == '1')]
# linechart(dfResultAnalise2019, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )
# dfResultAnalise2019 = dfResultAnalise[(dfResultAnalise['ano'] == 2019) & (dfResultAnalise['Fase'] == '2')]
# linechart(dfResultAnalise2019, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )

# dfResultAnalise2020 = dfResultAnalise[(dfResultAnalise['ano'] == 2020) ]
# linechart(dfResultAnalise2020, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )
# dfResultAnalise2020 = dfResultAnalise[(dfResultAnalise['ano'] == 2020) & (dfResultAnalise['Fase'] == '1')]
# linechart(dfResultAnalise2020, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )
# dfResultAnalise2020 = dfResultAnalise[(dfResultAnalise['ano'] == 2020) & (dfResultAnalise['Fase'] == '2')]
# linechart(dfResultAnalise2020, "Class_Exam", dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) )

# endregion



# Create bar chart with a series for each value of Fase
#dfResultados.groupby(['ano', 'Fase']).size().unstack().plot(kind='bar', stacked=True)

#dfResultados.groupby(['ano', 'Fase']).size().unstack().plot(kind='bar', stacked=False)
#dfResultados.groupby(['ano', 'Fase']).size().unstack().plot(kind='bar', stacked=False)
#plt.show()


#linechart(dfResultAnalise)




