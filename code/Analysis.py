import pandas as pd
from main import dicParams, dicFiles
from ImportUtils import vprint, vprint_time
#from ImportUtils import get_dfGeo_from_MDB, get_dfSchools_from_MDB, get_dfCursos_from_MDB, get_dfExames_from_MDB, get_dfResultados_from_MDB, get_dfResultAnalise_from_MDB
from ImportUtils import get_dfGeo_from_Parquet, get_dfSchools_from_Parquet, get_dfCursos_from_Parquet, get_dfExames_from_Parquet, get_dfResultados_from_Parquet, get_dfResultAnalise_from_Parquet





from main import dicParams, dicFiles
# Resultados dos Exames
# Apenas Fase 1


# Load datasets from parquet files
vprint("Loading Parquet...")
current_time = vprint_time(0, 'Before load datasets...')
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
#dfResultados    = get_dfResultados_from_Parquet()
#vprint("dfResultados.shape: ", dfResultados.shape)
#current_time = vprint_time(current_time, 'Loaded dfResultados from Parquet...')
#vprint("dfResultados.shape: ", dfResultados.shape)
#current_time = vprint_time(current_time, 'Loaded dfResultados from Parquet...')

dfResultAnalise = get_dfResultAnalise_from_Parquet()
vprint("dfResultAnalise.shape: ", dfResultAnalise.shape)
current_time = vprint_time(current_time, 'Loaded dfResultAnalise from Parquet...')

# List structure of dfResultAnalise dataframe
#vprint("dfResultAnalise.columns: ", dfResultAnalise.columns)

# Use matplolib to do these charts

#print(dfResultAnalise.head(5))

dfResultAnalise = dfResultAnalise[dfResultAnalise['ano'] == 2020]

#print(dfResultAnalise.head(5))

import matplotlib.pyplot as plt
dfResultAnalise['Class_Exam'].value_counts().sort_index().plot.line()


# Start x-axis at 0 and end at 200
plt.xlim(0, 200)

# Start y-axis at 0 and end automatically
plt.ylim(0, None)

# Plot x-axis marks from 0 to 200, step 10
plt.xticks(range(0, 200, 10))

# Set x-axis label to "Nota de exame"
plt.xlabel('Nota de exame')

# Set y-axis label to "Número de exames"
plt.ylabel('Número de exames')

# Set title to "Distribuição de notas de exame 1a fase, Secundário, 2008-2022"
plt.title('Distribuição de notas de exame 1a fase, Secundário, 2008-2022')

# Set horizontal grid lines
plt.grid(True)

# Show datapoint for 95 and 94, with number of exams and the datapoint value
# Calculate how many exams have a grade of 94
res94  = dfResultAnalise['Class_Exam'].value_counts().sort_index()[94]
res95  = dfResultAnalise['Class_Exam'].value_counts().sort_index()[95]
res200 = dfResultAnalise['Class_Exam'].value_counts().sort_index()[200]


# See https://stackoverflow.com/questions/14432557/matplotlib-scatter-plot-with-different-text-at-each-data-point
# Set dynamic position for the datapoint value

#plt.annotate('95', xy=(95, res95), xytext=(95, res95))
#plt.annotate('94', xy=(94, res94), xytext=(94, res94))
#plt.annotate('95', xy=(95, res95), xytext=(res95))
#plt.annotate('94', xy=(94, res94), xytext=(res94))

plt.text(94, res94, "9.4: " + str(res94))
plt.text(95, res95, "9.5: " + str(res95))
plt.text(200, res200, "20.0: " + str(res200))

#Write HELLO at coordinate x=95, y=res95
#plt.text(95, res95, 'HELLO')

# Write label XPTO for datapoint at coordinate x=95, y=res95



plt.show()



