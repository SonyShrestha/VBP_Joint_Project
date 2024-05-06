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
g.add((sb.customer, RDF.type, RDFS.Class))
g.add((sb.location, RDF.type, RDFS.Class))
g.add((sb.customer_purchase, RDF.type, RDFS.Class))
g.add((sb.product, RDF.type, RDFS.Class))


# Properties of class location
g.add((sb.location_id, RDFS.domain, sb.location))
g.add((sb.location_id, RDFS.range, XSD.string))
g.add((sb.location_id, RDF.type, RDF.Property))

g.add((sb.country_code, RDFS.domain, sb.location))
g.add((sb.country_code, RDFS.range, XSD.string))
g.add((sb.country_code, RDF.type, RDF.Property))

g.add((sb.postal_code, RDFS.domain, sb.location))
g.add((sb.postal_code, RDFS.range, XSD.string))
g.add((sb.postal_code, RDF.type, RDF.Property))

g.add((sb.place_name, RDFS.domain, sb.location))
g.add((sb.place_name, RDFS.range, XSD.string))
g.add((sb.place_name, RDF.type, RDF.Property))

g.add((sb.latitude, RDFS.domain, sb.location))
g.add((sb.latitude, RDFS.range, XSD.string))
g.add((sb.latitude, RDF.type, RDF.Property))

g.add((sb.longitude, RDFS.domain, sb.location))
g.add((sb.longitude, RDFS.range, XSD.string))
g.add((sb.longitude, RDF.type, RDF.Property))



# Properties of class customer
g.add((sb.customer_id, RDFS.domain, sb.customer))
g.add((sb.customer_id, RDFS.range, XSD.integer))
g.add((sb.customer_id, RDF.type, RDF.Property))

g.add((sb.customer_name, RDFS.domain, sb.customer))
g.add((sb.customer_name, RDFS.range, XSD.string))
g.add((sb.customer_name, RDF.type, RDF.Property))

g.add((sb.email_id, RDFS.domain, sb.customer))
g.add((sb.email_id, RDFS.range, XSD.string))
g.add((sb.email_id, RDF.type, RDF.Property))



g.add((sb.stays, RDFS.domain, sb.customer))
g.add((sb.stays, RDFS.range, sb.location))
g.add((sb.stays, RDF.type, RDF.Property))





# Properties of class customer_purchase
g.add((sb.purchase_customer, RDFS.domain, sb.customer_purchase))
g.add((sb.purchase_customer, RDFS.range, sb.customer))
g.add((sb.purchase_customer, RDF.type, RDF.Property))

g.add((sb.purchase_product, RDFS.domain, sb.customer_purchase))
g.add((sb.purchase_product, RDFS.range, sb.product))
g.add((sb.purchase_product, RDF.type, RDF.Property))

g.add((sb.purchased_date, RDFS.domain, sb.customer_purchase))
g.add((sb.purchased_date, RDFS.range, XSD.date))
g.add((sb.purchased_date, RDF.type, RDF.Property))

g.add((sb.unit_price, RDFS.domain, sb.customer_purchase))
g.add((sb.unit_price, RDFS.range, XSD.float))
g.add((sb.unit_price, RDF.type, RDF.Property))

g.add((sb.quantity, RDFS.domain, sb.customer_purchase))
g.add((sb.quantity, RDFS.range, XSD.float))
g.add((sb.quantity, RDF.type, RDF.Property))