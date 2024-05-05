from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD

# Define namespaces
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
pub = Namespace("http://www.example.edu/spicy_bytes/")

# Create an empty graph
g = Graph()

# Add triples to the graph
g.add((pub.supermarket, RDF.type, RDFS.Class))
g.add((pub.customer, RDF.type, RDFS.Class))
g.add((pub.location, RDF.type, RDFS.Class))
g.add((pub.product, RDF.type, RDFS.Class))
g.add((pub.recipe, RDF.type, RDFS.Class))
g.add((pub.inventory, RDF.type, RDFS.Class))

g.add((pub.person, RDF.type, RDFS.Class))
g.add((pub.customer, RDFS.subClassOf, pub.person))

g.add((pub.customer_purchase, RDF.type, RDFS.Class))

# Properties of class supermarket
g.add((pub.store_id, RDFS.domain, pub.supermarket))
g.add((pub.store_id, RDFS.range, XSD.string))
g.add((pub.store_id, RDF.type, RDF.Property))

g.add((pub.commercial_name, RDFS.domain, pub.supermarket))
g.add((pub.commercial_name, RDFS.range, XSD.string))
g.add((pub.commercial_name, RDF.type, RDF.Property))

g.add((pub.social_name, RDFS.domain, pub.supermarket))
g.add((pub.social_name, RDFS.range, XSD.string))
g.add((pub.social_name, RDF.type, RDF.Property))

g.add((pub.company_NIF, RDFS.domain, pub.supermarket))
g.add((pub.company_NIF, RDFS.range, XSD.string))
g.add((pub.company_NIF, RDF.type, RDF.Property))

g.add((pub.location, RDFS.domain, pub.supermarket))
g.add((pub.location, RDFS.range, XSD.string))
g.add((pub.location, RDF.type, RDF.Property))

g.add((pub.full_address, RDFS.domain, pub.supermarket))
g.add((pub.full_address, RDFS.range, XSD.string))
g.add((pub.full_address, RDF.type, RDF.Property))



# Properties of class location
g.add((pub.location_id, RDFS.domain, pub.location))
g.add((pub.location_id, RDFS.range, XSD.string))
g.add((pub.location_id, RDF.type, RDF.Property))

g.add((pub.country_code, RDFS.domain, pub.location))
g.add((pub.country_code, RDFS.range, XSD.string))
g.add((pub.country_code, RDF.type, RDF.Property))

g.add((pub.postal_code, RDFS.domain, pub.location))
g.add((pub.postal_code, RDFS.range, XSD.string))
g.add((pub.postal_code, RDF.type, RDF.Property))

g.add((pub.place_name, RDFS.domain, pub.location))
g.add((pub.place_name, RDFS.range, XSD.string))
g.add((pub.place_name, RDF.type, RDF.Property))

g.add((pub.latitude, RDFS.domain, pub.location))
g.add((pub.latitude, RDFS.range, XSD.string))
g.add((pub.latitude, RDF.type, RDF.Property))

g.add((pub.longitude, RDFS.domain, pub.location))
g.add((pub.longitude, RDFS.range, XSD.string))
g.add((pub.longitude, RDF.type, RDF.Property))



# Properties of class customer
g.add((pub.customer_id, RDFS.domain, pub.customer))
g.add((pub.customer_id, RDFS.range, XSD.integer))
g.add((pub.customer_id, RDF.type, RDF.Property))

g.add((pub.customer_name, RDFS.domain, pub.customer))
g.add((pub.customer_name, RDFS.range, XSD.string))
g.add((pub.customer_name, RDF.type, RDF.Property))

g.add((pub.email_id, RDFS.domain, pub.customer))
g.add((pub.email_id, RDFS.range, XSD.string))
g.add((pub.email_id, RDF.type, RDF.Property))

g.add((pub.stays, RDFS.domain, pub.customer))
g.add((pub.stays, RDFS.range, pub.location))
g.add((pub.stays, RDF.type, RDF.Property))



# Properties of class recipe
g.add((pub.food_name, RDFS.domain, pub.recipe))
g.add((pub.food_name, RDFS.range, XSD.string))
g.add((pub.food_name, RDF.type, RDF.Property))

g.add((pub.ingredients, RDFS.domain, pub.recipe))
g.add((pub.ingredients, RDFS.range, XSD.string))
g.add((pub.ingredients, RDF.type, RDF.Property))

g.add((pub.description, RDFS.domain, pub.recipe))
g.add((pub.description, RDFS.range, XSD.string))
g.add((pub.description, RDF.type, RDF.Property))



# Properties of class product
g.add((pub.product_name, RDFS.domain, pub.prouduct))
g.add((pub.product_name, RDFS.range, XSD.string))
g.add((pub.product_name, RDF.type, RDF.Property))

g.add((pub.avg_expiry_days, RDFS.domain, pub.prouduct))
g.add((pub.avg_expiry_days, RDFS.range, XSD.integer))
g.add((pub.avg_expiry_days, RDF.type, RDF.Property))

g.add((pub.belongs_to, RDFS.domain, pub.prouduct))
g.add((pub.belongs_to, RDFS.range, pub.subcategory))
g.add((pub.belongs_to, RDF.type, RDF.Property))

g.add((pub.belongs_to, RDFS.domain, pub.subcategory))
g.add((pub.belongs_to, RDFS.range, pub.category))
g.add((pub.belongs_to, RDF.type, RDF.Property))

g.add((pub.subcategory_name, RDFS.domain, pub.subcategory))
g.add((pub.subcategory_name, RDFS.range, XSD.string))
g.add((pub.subcategory_name, RDF.type, RDF.Property))

g.add((pub.category_name, RDFS.domain, pub.category))
g.add((pub.category_name, RDFS.range, pub.category))
g.add((pub.category_name, RDF.type, RDF.Property))



# Properties of class customer_purchase
g.add((pub.purchase_customer, RDFS.domain, pub.customer_purchase))
g.add((pub.purchase_customer, RDFS.range, pub.customer))
g.add((pub.purchase_customer, RDF.type, RDF.Property))

g.add((pub.purchase_product, RDFS.domain, pub.customer_purchase))
g.add((pub.purchase_product, RDFS.range, pub.product))
g.add((pub.purchase_product, RDF.type, RDF.Property))

g.add((pub.purchased_date, RDFS.domain, pub.customer_purchase))
g.add((pub.purchased_date, RDFS.range, XSD.date))
g.add((pub.purchased_date, RDF.type, RDF.Property))

g.add((pub.unit_price, RDFS.domain, pub.customer_purchase))
g.add((pub.unit_price, RDFS.range, XSD.float))
g.add((pub.unit_price, RDF.type, RDF.Property))

g.add((pub.quantity, RDFS.domain, pub.customer_purchase))
g.add((pub.quantity, RDFS.range, XSD.float))
g.add((pub.quantity, RDF.type, RDF.Property))


g.add((pub.of_supermarket, RDFS.domain, pub.inventory))
g.add((pub.of_supermarket, RDFS.range, pub.supermarket))
g.add((pub.of_supermarket, RDF.type, RDF.Property))

g.add((pub.of_product, RDFS.domain, pub.inventory))
g.add((pub.of_product, RDFS.range, pub.product))
g.add((pub.of_product, RDF.type, RDF.Property))

g.add((pub.expiry_date, RDFS.domain, pub.inventory))
g.add((pub.expiry_date, RDFS.range, XSD.date))
g.add((pub.expiry_date, RDF.type, RDF.Property))

g.add((pub.date, RDFS.domain, pub.inventory))
g.add((pub.date, RDFS.range, XSD.string))
g.add((pub.date, RDF.type, RDF.Property))

g.add((pub.remaining_quantity, RDFS.domain, pub.inventory))
g.add((pub.remaining_quantity, RDFS.range, XSD.integer))
g.add((pub.remaining_quantity, RDF.type, RDF.Property))

g.add((pub.unit_price, RDFS.domain, pub.inventory))
g.add((pub.unit_price, RDFS.range, XSD.float))
g.add((pub.unit_price, RDF.type, RDF.Property))


g.add((pub.product_name, RDFS.domain, pub.product))
g.add((pub.product_name, RDFS.range, XSD.string))
g.add((pub.product_name, RDF.type, RDF.Property))

