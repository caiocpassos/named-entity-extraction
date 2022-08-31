from py2neo import Graph
import spacy
import os


def read_text_file(file_path):
    with open(file_path, 'r', encoding=unicode) as f:
        dados = f.read()
        doc = nlp(dados)

        for entidade in doc.ents:
            entidadesDict[entidade.text] = entidade.label_, file


def createFile(entityFile: str):
    queryCreateFile = f"CREATE (f: File {{name: '{entityFile}'}})"
    grafo.run(queryCreateFile)


def createType(entityType: str):
    queryCreateType = f"CREATE (t: Type {{name: '{entityType}'}})"
    grafo.run(queryCreateType)


def createEntity(name: str):
    queryCreateEntity = f"CREATE (e: Entity {{name: '{name}'}})"
    grafo.run(queryCreateEntity)


def checkFile(entityFile: str):
    queryMatchFile = f"MATCH (f: File) WHERE f.name = '{entityFile}' RETURN f"
    listaFile = grafo.run(queryMatchFile).data()
    return listaFile


def checkType(entityType: str):
    queryMatchType = f"MATCH (t: Type) WHERE t.name = '{entityType}' RETURN t"
    listaType = grafo.run(queryMatchType).data()
    return listaType


def findType(entityType: str):
    queryFindType = f"MATCH (t: Type) WHERE t.name = '{entityType}' RETURN t.name"
    jsonListFindType = grafo.run(queryFindType).data()
    dictFindType = dict(jsonListFindType[0])
    typeName = str(dictFindType["t.name"])
    return typeName


def findEntity(entityName: str):
    queryMatchFile = f"MATCH (e: Entity) WHERE e.name = '{entityName}' RETURN e.name"
    jsonListFindEntity = grafo.run(queryMatchFile).data()
    dictFindEntity = dict(jsonListFindEntity[0])
    entityName = str(dictFindEntity["e.name"])
    return entityName


def findFile(entityFile: str):
    queryFindFile = f"MATCH (f: File) WHERE f.name = '{entityFile}' RETURN f.name"
    jsonListFindFile = grafo.run(queryFindFile).data()
    dictFindFile = dict(jsonListFindFile[0])
    fileName = str(dictFindFile["f.name"])
    return fileName


def createEntityFileRelationShip(entityName: str, entityFile: str):
    queryEntityFileRelationship = f"MATCH (a: Entity), (b: File) " \
                                  f"WHERE  a.name = '{entityName}' AND b.name = " \
                                  f"'{entityFile}' " \
                                  f"CREATE (a)-[r:Pertence " \
                                  f"{{name: a.name + 'belongs to' + b.name}}" \
                                  f"]->(b) " \
                                  f"RETURN type(r), r.name "

    grafo.run(queryEntityFileRelationship)


def createEntityTypeRelationShip(entityName: str, entityType: str):
    queryEntityTypeRelationship = f"MATCH (a: Entity), (b: Type) " \
                                  f"WHERE  a.name = '{entityName}' AND b.name = " \
                                  f"'{entityType}' " \
                                  f"CREATE (a)-[r:É " \
                                  f"{{name: a.name + 'is' + b.name}}" \
                                  f"]->(b) " \
                                  f"RETURN type(r), r.name "

    grafo.run(queryEntityTypeRelationship)


def createGraph(entityName: str, entityType: str, entityFile: str):
    listaFile = checkFile(entityFile)
    listaType = checkType(entityType)

    if len(listaFile) == 0:
        createFile(entityFile)
        recoveredFile = findFile(entityFile)

        if len(listaType) == 0:
            createType(entityType)
            createEntity(entityName)

            recoveredType = findType(entityType)
            recoveredEntity = findEntity(entityName)

            createEntityFileRelationShip(recoveredEntity, recoveredFile)
            createEntityTypeRelationShip(recoveredEntity, recoveredType)

        else:
            recoveredType = findType(entityType)

            createEntity(entityName)

            recoveredEntity = findEntity(entityName)

            createEntityFileRelationShip(recoveredEntity, recoveredFile)
            createEntityTypeRelationShip(recoveredEntity, recoveredType)

    else:
        recoveredFile = findFile(entityFile)

        if len(listaType) == 0:
            createType(entityType)
            createEntity(entityName)

            recoveredType = findType(entityType)
            recoveredEntity = findEntity(entityName)

            createEntityFileRelationShip(recoveredEntity, recoveredFile)
            createEntityTypeRelationShip(recoveredEntity, recoveredType)

        else:
            recoveredType = findType(entityType)

            createEntity(entityName)

            recoveredEntity = findEntity(entityName)

            createEntityFileRelationShip(recoveredEntity, recoveredFile)
            createEntityTypeRelationShip(recoveredEntity, recoveredType)


uri = "neo4j+s://483b2af7.databases.neo4j.io"
user = "neo4j"
password = "BwerOB20kumreP3c5a8ed_nBySrDSKzfdlGaJ9Xc2U8"

# windows-1252
unicode = 'windows-1252'

try:
    grafo = Graph(uri, auth=(user, password))
    print('SUCCESS: Connected to the Neo4j Database.')
except Exception as e:
    print('ERROR: Could not connect to the Neo4j Database. See console for details')
    raise SystemExit(e)

nlp = spacy.load("pt_core_news_sm")
path = r'D:\Projetos\named-entity-extraction\textos'
os.chdir(path)

entidadesDict = dict()

for file in os.listdir():
    if file.endswith(".txt"):
        file_path = f"{path}\{file}"
        read_text_file(file_path)

try:
    for key, value in entidadesDict.items():
        print(f"Nome = {key}")
        print(f"Tipo = {value[0]}")
        print(f"Arquivo = {value[1]}")
        print("\n")

        createGraph(key, value[0], value[1])
except Exception as e:
    print('UM ERRO OCORREU')
