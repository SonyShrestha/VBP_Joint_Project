import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace, RDF, XSD
from urllib.parse import quote
import datetime

# Define namespaces
pub = Namespace("http://www.example.edu/spicybytes/")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")

# Create an RDF graph
g = Graph()



def supermarkets():
    supermarkets_df = pd.read_parquet('./data/formatted_zone/establishments_catalonia')

    for index, row in supermarkets_df.iterrows():
        subject = URIRef(pub + 'supermarket/'+str(row['id']))

        commercial_name_literal = Literal(row['commercial_name'], datatype = XSD.string)
        social_name_literal = Literal(row['social_name'], datatype = XSD.string)
        company_nif_literal = Literal(row['company_NIF'], datatype = XSD.string)
        location_literal = Literal(row['location'], datatype = XSD.string)
        full_address_literal = Literal(row['full_address'], datatype = XSD.string)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.commercial_name, commercial_name_literal))
        g.add((subject, pub.social_name, social_name_literal))
        g.add((subject, pub.company_nif, company_nif_literal))
        g.add((subject, pub.location, location_literal))
        g.add((subject, pub.full_address, full_address_literal))

    return g



def supermarket_products():
    supermarket_products_df = pd.read_parquet('./data/formatted_zone/supermarket_products')
    supermarket_products_df = supermarket_products_df[['product_id', 'product_name']].drop_duplicates()

    for index, row in supermarket_products_df.iterrows():
        subject = URIRef(pub + 'product/' + row['product_id'])

        product_name_literal = Literal(row['product_name'], datatype = XSD.string)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.product_name, product_name_literal))

    return g



def inventory():
    inventory_df = pd.read_parquet('./data/formatted_zone/supermarket_products')

    for index, row in inventory_df.iterrows():
        subject = URIRef(pub + 'inventory/supermarket=' + row['store_id']+'/product='+row['product_id']+'/date='+str(datetime.date.today())) # to be modified later on

        manufacture_date_literal = Literal(row['manufacture_date'], datatype = XSD.string)
        expiry_date_literal = Literal(row['expiry_date'], datatype = XSD.string)
        quantity_literal = Literal(row['quantity'], datatype = XSD.string)
        supermarket = URIRef(pub + 'supermarket/'+str(row['store_id']))
        product = URIRef(pub + 'product/'+str(row['product_id']))
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.supermarket, supermarket))
        g.add((subject, pub.product, product))
        g.add((subject, pub.manufacture_date, manufacture_date_literal))
        g.add((subject, pub.expiry_date, expiry_date_literal))
        g.add((subject, pub.quantity, quantity_literal))

    return g



if __name__ == "__main__":
    supermarkets = supermarkets()
    supermarket_products = supermarket_products()
    inventory = inventory()
    kg = supermarkets + supermarket_products + inventory

    turtle = kg.serialize(format='turtle')
    kg.serialize(destination='./knowledge_graph/output/kg_abox_supermarket.rdf', format='xml')
    print(turtle)
