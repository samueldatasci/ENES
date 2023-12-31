# To run this project for the fist time:
<br>1. Create directories to receive files
<br>a) Create a directory C:\ENES
<br>OR
<br>1.b) In novaenes.yaml, change the keys dataFolder, dataFolderMDB, dataFolderParquet and dataFolderZIP to match your disk structure
<br>Data folder must already exist; the others will be created if needed.
<br>
<br>2. Run main.py
<br>By the end, your dataFolderParquet directory will have the needed parquet files
<br>If you delete some parquet files, just rerun main.py.
<br>

# Info
<br>
## Valor esperado
https://infoescolas.medu.pt/secundario/NI09.pdf

Ponto 2.6
No cálculo do indicador do alinhamento apenas são consideradas:
<br>a) as notas internas dos alunos da escola, # df["TemInterno"] == "S"
<br>b) matriculados em cursos Científico-Humanísticos, # df["SubtipoCurso"].isin(["N01"])
<br>c) que realizaram exames nacionais na 1a fase, # df["Fase"] == "1"
<br>d) para aprovação, # df["ParaAprov"] == "S"
<br>e) como alunos internos. # df["Interno"] == "S"
<br>f) Além disso, apenas são consideradas as notas internas das disciplinas em que o aluno obteve uma classificação superior ou igual a 9,5 valores no respetivo exame nacional # df["Class_Exam"] == "S"

##### NOTE1:
It could make sense to also just include dfAllFase1["CIF"] > 9
<br>However, this is automatically the case, because students with internal grade <= 9 take the exame as external students
<br>By adding this filter, the number of observations remains unchanged.

##### NOTE2:
We could consider other SubtipoCurso, such as N04 (same as N01, but for "Ensino Recorrente")
##### _List of values/description for curso subtipo N_
<br>SubTipo Descr
<br>N01 Cursos Científico-Humanísticos
<br>N02	Cursos Artísticos Especializados
<br>N03	Cursos Tecnológicos
<br>N04	Cursos Científico-Humanísticos do Ensino Recorrente
<br>N05	Cursos Tecnológicos do Ensino Recorrente
<br>N06	Cursos Artísticos Especializados do Ensino Recorrente
<br>N07	Cursos Profissionais
