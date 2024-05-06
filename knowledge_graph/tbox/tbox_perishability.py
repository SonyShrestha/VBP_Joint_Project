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
g.add((sb.perishability, RDF.type, RDFS.Class))

# Properties of class product
g.add((sb.product_name, RDFS.domain, sb.perishability))
g.add((sb.product_name, RDFS.range, XSD.string))
g.add((sb.product_name, RDF.type, RDF.Property))

g.add((sb.avg_expiry_days, RDFS.domain, sb.perishability))
g.add((sb.avg_expiry_days, RDFS.range, XSD.integer))
g.add((sb.avg_expiry_days, RDF.type, RDF.Property))

g.add((sb.category, RDFS.domain, sb.perishability))
g.add((sb.category, RDFS.range, XSD.string))
g.add((sb.category, RDF.type, RDF.Property))

g.add((sb.subcategory, RDFS.domain, sb.perishability))
g.add((sb.subcategory, RDFS.range, XSD.string))
g.add((sb.subcategory, RDF.type, RDF.Property))