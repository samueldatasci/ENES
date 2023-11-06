## DBI::


install.packages("RMySQL")
library(RMySQL)

  
dbconn <- dbConnect(MySQL(),
                 host = "localhost",
                 user = "root",
                 password = "****",
                 dbname = "NOVAENES")


# Get table District
query <- "select * from tblcodsdistrito"
result <- dbSendQuery(dbconn, "select * from tblcodsdistrito")
df <- dbFetch(result)


feed = dbSendQuery(dbconn, "Select * from tblcodsdistrito")
regionmajor <- dbFetch(feed)
regionmajor

feed = dbSendQuery(dbconn, "Select * from tblcodsconcelho")
regionminor <- dbFetch(feed)
regionminor

feed = dbSendQuery(dbconn, "Select * from tblescolas where distrito = 6 and concelho = 6") # Lisboa
schools <- dbFetch(feed)
schools

notas
mod <- lm(CIF ~ Escola, Fase, Sexo, Class_Exam, data=notas)
mod <- lm(CIF ~ Fase, data=notas)
mod

feed = dbSendQuery(dbconn, "Select notas.* from tblnotas notas inner join tblescolas escolas on notas.escola = escolas.escola") # Lisboa
notas <- dbFetch(feed)
notas

notas <- schools

feed = dbSendQuery(dbconn, "Select notas.* from tblnotas notas where notas.escola = escolas.escola where notas.AnoExame = 2019") # Lisboa
schools <- dbFetch(feed)
schools


mod <- lm(CIF ~ Escola, Fase, Sexo, Class_Exame, data=notas)
md = summary(mod)
md


#####

feed = dbSendQuery(dbconn, "Select grades.* from tblnotas grades where grades.AnoExame = 2019");
grades <- dbFetch(feed)
#dbClearResult(feed)
grades


mod <- lm(CIF ~  Fase + Sexo, data=grades)
mod <- lm(CIF ~ Sexo, data=grades)
summary(mod)

grades$Fase



dbDisconnect(dbconn)
