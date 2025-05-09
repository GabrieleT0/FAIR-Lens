from lxml import etree
import requests
from io import BytesIO
from urllib.parse import urlparse
import json
import os
import pandas as pd

namespaces = {
    'svg': 'http://www.w3.org/2000/svg',
    'xlink': 'http://www.w3.org/1999/xlink'
}

class SplitLODCKGsByTopic:
    def __init__(self):
        self.svg_links = ['https://lod-cloud.net/versions/latest/cross-domain-lod.svg','https://lod-cloud.net/versions/latest/geography-lod.svg','https://lod-cloud.net/versions/latest/government-lod.svg',
                          'https://lod-cloud.net/versions/latest/life-sciences-lod.svg','https://lod-cloud.net/versions/latest/linguistic-lod.svg','https://lod-cloud.net/versions/latest/media-lod.svg',
                          'https://lod-cloud.net/versions/latest/publications-lod.svg','https://lod-cloud.net/versions/latest/social-networking-lod.svg','https://lod-cloud.net/versions/latest/user-generated-lod.svg']
    
    def recover_lodc_kgs_by_topic(self):
        '''
            Retrieves svg files from LOD Cloud and extracts links to KG metadata.
        '''
        kgs_by_topic = {}
        for link in self.svg_links:
            response = requests.get(link)
            svg_content = response.content

            svg_content = svg_content.decode("utf-8")
            
            tree = etree.parse(BytesIO(svg_content.encode("utf-8")))

            # Find all <g> tags that contains <a>
            g_elements = tree.xpath("//svg:g[svg:a]", namespaces=namespaces)   

            # Extract the <a> tags in the <g>
            hrefs = []
            for g in g_elements:
                a_element = g.find(".//svg:a", namespaces) 
                if a_element is not None:
                    href = a_element.get("href") 
                    if href:
                        hrefs.append(href)
            
            
            topic = ((urlparse(link).path.split("/")[-1]).split('.')[0]).replace('-lod','')
            kgs_by_topic[topic] = hrefs  
        
        for topic in kgs_by_topic:
            print(f"Number of dataset in the topic {topic}: {len(kgs_by_topic[topic])}")

        with open('../data/kgs_by_topic.json','w',encoding='utf-8') as file: 
            json.dump(kgs_by_topic, file, indent=4, ensure_ascii=False)    

    def split_kgs_csv_by_topic(self,dir_path):
        '''
            Extract the KGs from LODCloud and split it by topic in different folder.

            :param dir_path: path to csv where to get the KGs to split by topic.
        '''
        self.recover_lodc_kgs_by_topic()
        with open('../data/kgs_by_topic.json', "r", encoding="utf-8") as file:
            kgs_by_topic_dict = json.load(file)

        for topic in kgs_by_topic_dict:
            kgs_in_the_topic = kgs_by_topic_dict[topic]
            kgs_in_the_topic = [url.split("/")[-1] for url in kgs_in_the_topic]

            for filename in os.listdir(dir_path):
                if '.csv' in filename:
                    file_path = os.path.join(dir_path, filename)
                    df = pd.read_csv(file_path)

                    identifiers_in_csv = set(df['KG id'].unique())
                    missing_identifiers = set(kgs_in_the_topic) - identifiers_in_csv

                    print(f"File: {file_path} filtered")
                    print(f"For topic: {topic} {len(missing_identifiers)} KGs not analyzed by KGHB")

                    df['KG id'] = df['KG id'].astype(str).str.strip()
                    df_filtered = df[df['KG id'].isin(kgs_in_the_topic)]

                    df_filtered.to_csv(f"../data/quality_data/{topic}/{filename}",index=False)

        # Create a CSVs with only yhe KGs without a domain
        all_kgs_without_domain = []
        for urls in kgs_by_topic_dict.values():
            all_kgs_without_domain.extend(urls)
        all_kgs_without_domain = [url.split("/")[-1] for url in all_kgs_without_domain]
        for filename in os.listdir(dir_path):
                if '.csv' in filename:
                    file_path = os.path.join(dir_path, filename)
                    df = pd.read_csv(file_path)

                    df['KG id'] = df['KG id'].astype(str).str.strip()
                    df_filtered = df[~df['KG id'].isin(all_kgs_without_domain)]

                    df_filtered.to_csv(f"../data/quality_data/no-domain/{filename}",index=False)
    
        def extract_only_lodc(self,analysis_results_path):
            '''
                Extract only KGs from LODCloud from the csv output from KGs Quality Analyzer.

                :param analysis_results_path: path to csv where to discard the KGs.
            '''
            try:
                response = requests.get("https://lod-cloud.net/versions/latest/lod-data.json")
                kgs = response.json()
                with open('../data/lodcloud.json', "r", encoding="utf-8") as f:
                    json.dump(kgs, f, indent=4)
                print(f"{len(kgs)} KGs recovered from the LOD Cloud")
            except:
                with open('../data/lodcloud.json', "r", encoding="utf-8") as file:
                    kgs = json.load(file)
                    print(f"{len(kgs)} KGs recovered from the LOD Cloud")
        
            identifiers = [data['identifier'] for key, data in kgs.items()]
            # Iterate throught all the csv and create a new csv with only the KGs from LODCloud
            for filename in os.listdir(analysis_results_path):
                if '.csv' in filename:
                    file_path = os.path.join(analysis_results_path, filename)
                    df = pd.read_csv(file_path)

                    identifiers_in_csv = set(df['KG id'].unique())
                    missing_identifiers = set(identifiers) - identifiers_in_csv

                    print(f"File: {file_path} filtered")
                    print(f"{len(missing_identifiers)} KGs not analyzed by KGHeartBeat")

                    df['KG id'] = df['KG id'].astype(str).str.strip()
                    df_filtered = df[df['KG id'].isin(identifiers)]

                    df_filtered.to_csv(f"../data/quality_data/all/{filename}",index=False)

