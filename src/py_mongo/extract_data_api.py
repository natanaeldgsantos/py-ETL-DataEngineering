import pandas as pd
import requests
import json



# BUSCANDO DADOS DA API CHUCK NORRIS

categories_endpoint = 'https://api.chucknorris.io/jokes/categories'
random_endpoint     = 'https://api.chucknorris.io/jokes/random?category={category}'

def get_all_categories():

    response = requests.get(categories_endpoint)
    categories= response.json()
    
    print('Categories:')
    for value in categories:
        print(value, end=", ")
    


def get_random_facts():

    response = requests.get(random_endpoint.format(category = 'political'))
    n = response.json()
    print(type(n))

get_random_facts()



