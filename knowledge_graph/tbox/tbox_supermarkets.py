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
g.add((sb.supermarket, RDF.type, RDFS.Class))
g.add((sb.product, RDF.type, RDFS.Class))
g.add((sb.inventory, RDF.type, RDFS.Class))

# Properties of class supermarket
g.add((sb.store_id, RDFS.domain, sb.supermarket))
g.add((sb.store_id, RDFS.range, XSD.string))
g.add((sb.store_id, RDF.type, RDF.Property))

g.add((sb.commercial_name, RDFS.domain, sb.supermarket))
g.add((sb.commercial_name, RDFS.range, XSD.string))
g.add((sb.commercial_name, RDF.type, RDF.Property))

g.add((sb.social_name, RDFS.domain, sb.supermarket))
g.add((sb.social_name, RDFS.range, XSD.string))
g.add((sb.social_name, RDF.type, RDF.Property))

g.add((sb.company_NIF, RDFS.domain, sb.supermarket))
g.add((sb.company_NIF, RDFS.range, XSD.string))
g.add((sb.company_NIF, RDF.type, RDF.Property))

g.add((sb.location, RDFS.domain, sb.supermarket))
g.add((sb.location, RDFS.range, XSD.string))
g.add((sb.location, RDF.type, RDF.Property))

g.add((sb.full_address, RDFS.domain, sb.supermarket))
g.add((sb.full_address, RDFS.range, XSD.string))
g.add((sb.full_address, RDF.type, RDF.Property))




# Properties of class product
g.add((sb.product_name, RDFS.domain, sb.prouduct))
g.add((sb.product_name, RDFS.range, XSD.string))
g.add((sb.product_name, RDF.type, RDF.Property))



g.add((sb.of_supermarket, RDFS.domain, sb.inventory))
g.add((sb.of_supermarket, RDFS.range, sb.supermarket))
g.add((sb.of_supermarket, RDF.type, RDF.Property))

g.add((sb.of_product, RDFS.domain, sb.inventory))
g.add((sb.of_product, RDFS.range, sb.product))
g.add((sb.of_product, RDF.type, RDF.Property))

g.add((sb.expiry_date, RDFS.domain, sb.inventory))
g.add((sb.expiry_date, RDFS.range, XSD.date))
g.add((sb.expiry_date, RDF.type, RDF.Property))

g.add((sb.date, RDFS.domain, sb.inventory))
g.add((sb.date, RDFS.range, XSD.string))
g.add((sb.date, RDF.type, RDF.Property))

g.add((sb.remaining_quantity, RDFS.domain, sb.inventory))
g.add((sb.remaining_quantity, RDFS.range, XSD.integer))
g.add((sb.remaining_quantity, RDF.type, RDF.Property))

g.add((sb.unit_price, RDFS.domain, sb.inventory))
g.add((sb.unit_price, RDFS.range, XSD.float))
g.add((sb.unit_price, RDF.type, RDF.Property))
