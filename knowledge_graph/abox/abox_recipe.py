import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace, RDF, XSD
from urllib.parse import quote

# Define namespaces
pub = Namespace("http://www.example.edu/spicybytes/")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")

# Create an RDF graph
g = Graph()



def recipe():
    recipe_df = pd.read_parquet('./data/formatted_zone/mealdb')
    recipe_df['food_name_quoted'] = recipe_df['food_name'].str.lower().apply(quote)

    for index, row in recipe_df.iterrows():
        subject = URIRef(pub + 'recipe/'+str(row['food_name_quoted']))

        food_name_literal = Literal(row['food_name'], datatype = XSD.string)
        description_literal = Literal(row['description'], datatype = XSD.string)

        for ingredient in row['ingredients']:
            ingredient_literal = Literal(ingredient, datatype = XSD.string)
            g.add((subject, pub.ingredient, ingredient_literal))
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.food_name, food_name_literal))
        g.add((subject, pub.description, description_literal))

    return g


if __name__ == "__main__":
    recipe = recipe()

    turtle = recipe.serialize(format='turtle')
    recipe.serialize(destination='./knowledge_graph/output/kg_abox_recipe.rdf', format='xml')
