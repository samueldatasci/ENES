library(RMySQL)


dbconn <- dbConnect(MySQL(),
                    host = "localhost",
                    user = "root",
                    password = "rute",
                    dbname = "NOVAENES")


feed = dbSendQuery(dbconn, "Select * from tblcodsdistrito")
regionmajor <- dbGetQuery(feed)
dbClearResult(feed)
regionmajor

feed = dbSendQuery(dbconn, "Select * from tblcodsconcelho")
regionminor <- dbFetch(feed)
dbClearResult(feed)
regionminor

feed = dbSendQuery(dbconn, "Select * from tblescolas where distrito = 11 and concelho = 6") # Just schools in Lisboa
schools <- dbFetch(feed)
dbClearResult(feed)
schools

#feed = dbSendQuery(dbconn, "Select grades.* from tblnotas grades where AnoExame = 2019")
feed = dbSendQuery(dbconn, "Select grades.* from tblnotas grades where AnoExame = 2019 and Fase=1")
#Next 2 commands take a bit to run
grades <- dbFetch(feed)
#dbClearResult(feed)
grades


modCIF <- lm(CIF ~  Sexo + I(AnoExame-2008) + Fase, data=grades)
summary(modCIF)
modExam <- lm(Class_Exam ~  Sexo + I(AnoExame-2008) + Fase + I(as.integer(Escola)), data=grades)
summary(modExam)
modExam <- lm(Class_Exam ~  Sexo + Fase + I(as.integer(Escola)), data=grades)
summary(modExam)


modExam <- lm(Class_Exam ~  Sexo + I(AnoExame-2008) + Fase + I(as.integer(Escola)), data=grades)
summary(modExam)

feed = dbSendQuery(dbconn, "Select grades.* from tblnotas grades where class_exam=94 or class_exam = 95")
#Next 2 commands take a bit to run
grades <- dbFetch(feed)

modExam <- lm(Class_Exam  ~  Sexo + I(AnoExame-2008) + Fase, data=grades)
summary(modExam)
grades

dbClearResult(feed)
dbDisconnect(dbconn)
