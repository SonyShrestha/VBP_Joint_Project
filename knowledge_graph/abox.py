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


def expected_avg_expiry():
    expiry_df = pd.read_parquet('./data/formatted_zone/estimated_avg_expiry')

    for index, row in expiry_df.iterrows():
        subject = URIRef(pub + 'perishability/'+str(row['product_name']).replace(' ','_'))

        category_literal = Literal(row['category'], datatype = XSD.string)
        subcategory_literal = Literal(row['sub_category'], datatype = XSD.string)
        avg_expiry_days_literal = Literal(row['avg_expiry_days'], datatype = XSD.integer)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.category, category_literal))
        g.add((subject, pub.subcategory, subcategory_literal))
        g.add((subject, pub.avg_expiry_days, avg_expiry_days_literal))

    return g

def product():
    product_df = pd.read_parquet('./data/formatted_zone/customer_purchase')
    product_df = pd.DataFrame({'product_name': product_df['product_name'].unique()})
    product_df['product_name_quoted'] = product_df['product_name'].str.lower().apply(quote)

    for index, row in product_df.iterrows():
        subject = URIRef(pub + 'product/'+str(row['product_name_quoted']).lower().replace(' ','_'))

        product_name_literal = Literal(row['product_name'], datatype = XSD.string)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.product_name, product_name_literal))

    return g


def customer_purchase():
    customer_purchase_df = pd.read_parquet('./data/formatted_zone/customer_purchase')
    customer_purchase_df['product_name'] = customer_purchase_df['product_name'].str.lower().apply(quote)

    for index, row in customer_purchase_df.iterrows():
        subject = URIRef(pub + 'customer_purchase/customer='+str(row['customer_id'])+"/product="+str(row['product_name']).lower().replace(' ','_'))

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
    # customers = customers()
    # expected_avg_expiry = expected_avg_expiry()
    # product = product()
    customer_purchase = customer_purchase()
    # kg = customers + expected_avg_expiry + product + customer_purchase
    # kg.serialize(destination='./output/kg_abox.rdf', format='xml')
    
    # g=conference_publication()

    turtle = customer_purchase.serialize(format='turtle')
    customer_purchase.serialize(destination='./knowledge_graph/output/kg_abox.rdf', format='xml')
    print(turtle)
