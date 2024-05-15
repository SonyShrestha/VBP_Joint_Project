import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace, RDF, XSD
from urllib.parse import quote

# Define namespaces
pub = Namespace("http://www.example.edu/spicybytes/")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")

# Create an RDF graph
g = Graph()



def customers():
    customers_df = pd.read_parquet('./data/formatted_zone/customers')

    for index, row in customers_df.iterrows():
        subject = URIRef(pub + 'customer/'+str(row['customer_id']))

        customer_name_literal = Literal(row['customer_name'], datatype = XSD.string)
        email_id_literal = Literal(row['email_id'], datatype = XSD.string)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.customer_name, customer_name_literal))
        g.add((subject, pub.email_id, email_id_literal))

    return g



def location():
    location_df = pd.read_parquet('./data/formatted_zone/location')

    for index, row in location_df.iterrows():
        subject = URIRef(pub + 'location/'+str(row['location_id']))

        country_code_literal = Literal(row['country_code'], datatype = XSD.string)
        postal_code_literal = Literal(row['postal_code'], datatype = XSD.string)
        place_name_literal = Literal(row['postal_code'], datatype = XSD.string)
        latitude_literal = Literal(row['latitude'], datatype = XSD.string)
        longitude_literal = Literal(row['longitude'], datatype = XSD.string)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.country_code, country_code_literal))
        g.add((subject, pub.postal_code, postal_code_literal))
        g.add((subject, pub.place_name, place_name_literal))
        g.add((subject, pub.latitude, latitude_literal))
        g.add((subject, pub.longitude, longitude_literal))

    return g



def customer_location():
    customer_location_df = pd.read_parquet('./data/formatted_zone/customer_location')

    for index, row in customer_location_df.iterrows():
        subject = URIRef(pub + 'stays/customer='+str(row['customer_id'])+'/location_id='+str(row['location_id']))

        customer = URIRef(pub + 'customer/'+str(row['customer_id']))
        location = URIRef(pub + 'location/'+str(row['location_id']))
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.stays, customer))
        g.add((subject, pub.stays, location))

    return g

def customer_purchase():
    customer_purchase_df = pd.read_parquet('./data/formatted_zone/customer_purchase')
    customer_purchase_df['product_name'] = customer_purchase_df['product_name'].str.lower().apply(quote)

    for index, row in customer_purchase_df.iterrows():
        subject = URIRef(pub + 'customer_purchase/customer='+str(row['customer_id'])+"/product="+str(row['product_name']))

        purchased_date_literal = Literal(row['purchase_date'], datatype = XSD.date)
        quantity_literal = Literal(row['quantity'], datatype = XSD.integer)
        unit_price_literal = Literal(row['unit_price'], datatype = XSD.float)
        customer = URIRef(pub + 'customer/'+str(row['customer_id']))
        product = URIRef(pub + 'product/'+str(row['product_name']))
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.purchased_date, purchased_date_literal))
        g.add((subject, pub.quantity, quantity_literal))
        g.add((subject, pub.unit_price, unit_price_literal))
        g.add((subject, pub.purchase_customer, customer))
        g.add((subject, pub.purchase_product, product))

    return g



if __name__ == "__main__":
    customers = customers()
    location = location()
    customer_location = customer_location()
    customer_purchase = customer_purchase()
    kg = customers + location + customer_location + customer_purchase

    turtle = kg.serialize(format='turtle')
    kg.serialize(destination='./knowledge_graph/output/kg_abox_customers.rdf', format='xml')
    print(turtle)
