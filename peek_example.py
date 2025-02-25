from neo4j import GraphDatabase, basic_auth
import os 
import dotenv

dotenv.load_dotenv()

def get_actors_consume(tx, name):
    result = tx.run("""
        MERGE (p:Person {name: $name})
        RETURN p
    """, name=name)

    info = result.consume()

def get_actors_values(tx, movie):
    result = tx.run("""
        MATCH (p:Person)-[r:ACTED_IN]->(m:Movie {title: $title})
        RETURN p.name AS name, m.title AS title, r.roles AS roles
    """, title=movie)

    return result.value("name", False)

# Unit of work
def get_actors(tx, movie): # (1)
    query = """
    MATCH (p:Person)-[:ACTED_IN]->(m:Movie) 
    RETURN p LIMIT 10
"""

    result = tx.run(query, movie=movie)
    # Access the `p` value from each record
    print(movie)
    peek = result.peek()
    print("Peek Output", peek)
    print("Result Keys",result.keys())
    print("Single result",result.single())
    return [ record["p"] for record in result ]

def init_driver(uri, username, password):
    # TODO: Create an instance of the driver here
    driver = GraphDatabase.driver( uri,
    auth=basic_auth(username, password))
    driver.verify_connectivity()
    return driver
# end::initDriver[]
# Open a Session


if __name__=="__main__":
    NEO4J_USERNAME=os.getenv('NEO4J_USERNAME')
    NEO4J_PASSWORD=os.getenv('NEO4J_PASSWORD')
    driver=init_driver(uri=os.environ.get('NEO4J_URI'),username=NEO4J_USERNAME,password=NEO4J_PASSWORD)
    with driver.session() as session:
        # Run the unit of work within a Read Transaction
        actors = session.execute_read(get_actors, movie="The Green Mile") # (2)
        for record in actors:
            print(record["p"])

        session.close()
