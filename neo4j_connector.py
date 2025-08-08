from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

# Inserisci qui le tue credenziali di Neo4j Aura
conn = Neo4jConnection(
    uri= "neo4j+s://b09177c2.databases.neo4j.io",
    user="neo4j",
    password="QfMljDUWmK73k5zPI0whDNtvj7BAo-A6lyUXc1sWlL8"
)
