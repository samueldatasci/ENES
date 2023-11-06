

library(readr)
library(dplyr)
library(arrow)

# Define the path to the parquet file
k_file <- "C:/ENES/PARQUET/dfInfoEscolas.parquet.gzip"

# Read in the parquet file
df <- arrow::read_parquet(k_file)

# Prepare the data for linear regression
df <- df %>%
  select( CIF_bonus_mean_ExamRounded, Sexo, ano, Exame, PubPriv, Curso ) 
  # %>%
  # select the variables you want to use in the regression
#  na.omit() # remove any rows with missing data


modBonus <- lm(CIF_bonus_mean_ExamRounded ~  Sexo + I(ano-2008) + Exame + PubPriv, data=df)
summary(modCIF)


# table(df$CIF_bonus_mean_ExamRounded)
# print(table(df$Sexo))
# print(table(df$ano))

# print(table(df$Exame))
# table(df$PubPriv)
# table(df$Curso)

#modExam <- lm(Class_Exam ~  Sexo + I(AnoExame-2008) + I(as.integer(Escola)), data=grades)
#modExam <- lm(Class_Exam  ~  Sexo + I(AnoExame-2008) + Fase, data=grades)




# Index(['ano', 'ID', 'Escola', 'Fase', 'Exame', 'ParaAprov', 'Interno',
#        'ParaMelhoria', 'ParaIngresso', 'ParaCFCEPE', 'TemInterno', 'Sexo',
#        'Idade', 'Curso', 'SitFreq', 'CIF', 'Class_Exam', 'CFD',
#        'Class_Exam_Rounded', 'Class_Exam_RoundUp', 'Covid', 'AnoDadosEscola',
#        'Distrito', 'Concelho', 'DescrEscola', 'PubPriv', 'CodDGEEC', 'Descr',
#        'DescrDistrito', 'DescrConcelho', 'Nuts3', 'DescrNuts3', 'Nuts2',
#        'DescrNuts2', 'DescrExame', 'TipoExame', 'DescrExameAbrev',
#        'SitFreqDescr', 'SitFreqDefin', 'TipoCurso', 'SubtipoCurso',
#        'DescrCurso', 'DescrTipoCurso', 'TipoCurso_Ano_Ini',
#        'TipoCurso_Ano_Term', 'TipoCurso_Ordena', 'DescrSubtipoCurso',
#        'ExamRounded_median_CIF', 'ExamRounded_mean_CIF',
#        'ExamRounded_count_CIF', 'ExamRoundUp_median_CIF',
#        'ExamRoundUp_mean_CIF', 'ExamRoundUp_count_CIF',
#        'CIF_bonus_median_ExamRounded', 'CIF_bonus_mean_ExamRounded',
#        'CIF_bonus_median_ExamRoundUp', 'CIF_bonus_mean_ExamRoundUp',
#        'CIF_bonus_adj_median_ExamRounded', 'CIF_bonus_adj_mean_ExamRounded',
#        'CIF_bonus_adj_median_ExamRoundUp', 'CIF_bonus_adj_mean_ExamRoundUp'],
#       dtype='object')


