# To run this project for the fist time:
1. Create directories to receive files
a) Create a directory C:\ENES
OR
1.b) In novaenes.yaml, change the keys dataFolder, dataFolderMDB, dataFolderParquet and dataFolderZIP to match your disk structure
Data folder must already exist; the others will be created if needed.

2. Run main.py
By the end, your dataFolderParquet directory will have the needed parquet files
If you delete some parquet files, just rerun main.py.


# Info

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
SubTipo Descr
N01 Cursos Científico-Humanísticos
N02	Cursos Artísticos Especializados
N03	Cursos Tecnológicos
N04	Cursos Científico-Humanísticos do Ensino Recorrente
N05	Cursos Tecnológicos do Ensino Recorrente
N06	Cursos Artísticos Especializados do Ensino Recorrente
N07	Cursos Profissionais
