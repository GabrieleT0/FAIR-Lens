import os
import json
import pandas as pd
import re
import requests
from SPARQLWrapper import *
from SPARQLWrapper import SPARQLWrapper
from fair_vocabularies import fair_vocabularies
from scipy.stats import shapiro

def check_if_ontology(kg_id,path_to_lodcloud_data_to_use = '../data/lodcloud.json'):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here,path_to_lodcloud_data_to_use), "r", encoding="utf-8") as file:
        lodcloud_data = json.load(file)
    
    for key in lodcloud_data:
        dataset = lodcloud_data[key]
        if kg_id == dataset['identifier']:
            keywords = dataset.get('keywords','')
            if 'ontology' in keywords:
                return True
            else: 
                return False
            
def recover_doi_from_lodcloud(kg_id, path_to_lodcloud_data_to_use = '../data/lodcloud.json'):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here,path_to_lodcloud_data_to_use), "r", encoding="utf-8") as file:
        lodcloud_data = json.load(file)
    for key in lodcloud_data:
        kg_metadata = lodcloud_data[key]
        if kg_id == kg_metadata['identifier']:
            doi = kg_metadata.get('doi','')
            if doi != '':
                return 1
            else:
                return 0

def check_publisher_info(row):
    author_query = 1 if pd.notna(row['Author (query)']) and row['Author (query)'] not in ['[]', '-'] else 0
    
    author_metadata = 0
    if pd.notna(row['Author (metadata)']) and row['Author (metadata)'] not in [False,'False']:
        if not re.fullmatch(r"Name:\s*absent,\s*email:\s*absent", row['Author (metadata)'], re.IGNORECASE):
            author_metadata = 1
    
    contributors = 1 if pd.notna(row['Contributor']) and row['Contributor'] not in ['[]', '-'] else 0

    publishers = 1 if pd.notna(row['Publisher']) and row['Publisher'] not in ['[]', '-'] else 0

    sources = 0
    if pd.notna(row['Sources']) and row['Sources'] not in ['-', '']:
        # Extract values after "Web:", "Name:", and "Email:"
        matches = re.findall(r"(?:Web|Name|Email):\s*([^,]+)", row['Sources'], re.IGNORECASE)
        # Check if any extracted value is not "absent" or empty
        if any(value.strip().lower() not in ["absent", "", 'Absent'] for value in matches):
            sources = 1
    

    return 1 if author_query or author_metadata or contributors or  publishers or sources else 0

def check_at_least_sparql_on(sparql_url):
    '''
    Check if the SPARQL endpoint return a 200 status, also if the sparql editor is not interoperable
    '''
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(sparql_url, headers=headers, timeout=10,verify=False)

        if 200 <= response.status_code < 300:
            return 1
        else:
            return 0
        
    except requests.exceptions.RequestException as e:
        # Handle any exceptions that may occur
        return 0

def check_meta_in_sparql(endpoint_url):
    sparql = SPARQLWrapper(endpoint_url)
    query = """
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcat: <http://www.w3.org/ns/dcat#>

    SELECT DISTINCT ?s
    WHERE {
    {
        ?s a void:Dataset .
    }
    UNION
    {
        ?s a dcat:Dataset .
    }
    }
    """
    sparql.setQuery(query)
    sparql.setTimeout(300)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
        if isinstance(results,dict):
            result = results.get('results')
            bindings = result.get('bindings')
            if isinstance(bindings,list) and len(bindings) > 0:
                return 1
            else:
                return 0
        elif isinstance(results,Document):
            li = []
            literalList = results.getElementsByTagName('literal')
            numTags = results.getElementsByTagName("literal").length
            for i in range(numTags):
                if literalList[i].firstChild is not None:
                    literal = literalList[i].firstChild.nodeValue
                    li.append(literal)
            if len(li) > 0:
                return 1
            else:
                return 0
    except:
        return 0

def check_if_fair_vocabs(vocabs):
    vocabs = vocabs.replace('[','')
    vocabs = vocabs.replace(']','')
    vocabs = vocabs.split(',')
    total_vocabs = len(vocabs)
    fair_vocabularies_defined = []
    for vocab in vocabs:
        vocab = vocab.strip()
        vocab = vocab.replace("'","")
        vocab = vocab.replace('"',"")
        if vocab in fair_vocabularies:
            fair_vocabularies_defined.append(vocab)
    return len(fair_vocabularies_defined) / total_vocabs if total_vocabs > 0 else 0

def verify_normal_distribution(csv_file_path,columns_to_verify):
    df = pd.read_csv(csv_file_path)
    for col in columns_to_verify:
        data = df[col].dropna() 
        stat, p = shapiro(data)
        print(f'Statistic: {stat}, p-value: {p}')
        if p < 0.05:
            print("Not all column are normally distributed")
            return False
    print("All column are normally distributed")
    return True