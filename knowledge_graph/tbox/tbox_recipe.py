from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD

# Define namespaces
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
sb = Namespace("http://www.example.edu/spicy_bytes/")

# Create an empty graph
g = Graph()

# Add triples to the graph
g.add((sb.recipe, RDF.type, RDFS.Class))

# Properties of class recipe
g.add((sb.food_name, RDFS.domain, sb.recipe))
g.add((sb.food_name, RDFS.range, XSD.string))
g.add((sb.food_name, RDF.type, RDF.Property))

g.add((sb.ingredients, RDFS.domain, sb.recipe))
g.add((sb.ingredients, RDFS.range, XSD.string))
g.add((sb.ingredients, RDF.type, RDF.Property))

g.add((sb.description, RDFS.domain, sb.recipe))
g.add((sb.description, RDFS.range, XSD.string))
g.add((sb.description, RDF.type, RDF.Property))
