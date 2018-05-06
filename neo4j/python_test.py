from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

def add_person(name):
    with driver.session() as session:
        session.run("CREATE (a:Person {name: $name})", name=name)

def print_friends_of(name):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                                 "WHERE a.name = {name} "
                                 "RETURN f.name", name=name):
                print(record["f.name"])

def create_friendship(name_a, name_b):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (a:Person),(b:Person) "
                                 "WHERE a.name = {name_a} AND b.name = {name_b} "
                                 "CREATE (a)-[r:KNOWS]->(b) "
                                 "RETURN a.name, type(r), b.name", name_a = name_a, name_b = name_b):
                print(record["a.name"], record["type(r)"], record["b.name"])

def delete_graph():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            tx.run("MATCH(n) "
                   "DETACH DELETE n ")

delete_graph()
add_person("Alice")
add_person("Bob")
add_person("John")
create_friendship("Alice", "Bob")
create_friendship("Alice", "John")
print_friends_of("Alice")
