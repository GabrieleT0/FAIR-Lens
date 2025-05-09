import requests
import json
import os
import pandas as pd

namespaces = {
    'svg': 'http://www.w3.org/2000/svg',
    'xlink': 'http://www.w3.org/1999/xlink'
}

class SplitLODCKGsByTopic:
    def __init__(self,kghb_quality_data_path):
        self.kghb_quality_data_path = kghb_quality_data_path
        try:
            response = requests.get("https://lod-cloud.net/versions/2025-03-26/lod-data.json")
            response.raise_for_status() 
            self.lodcloud_data = response.json()
            with open('../data/lodcloud.json', "w", encoding="utf-8") as f:
                json.dump(self.lodcloud_data, f, indent=4)
            print(f"{len(self.lodcloud_data)} KGs recovered from the LOD Cloud")
        except Exception as e:
            print(e)
            with open('../data/lodcloud.json', "r", encoding="utf-8") as file:
                self.lodcloud_data = json.load(file)
                print(f"{len(self.lodcloud_data)} KGs recovered from the LOD Cloud")
    
    def recover_lodc_kgs_by_topic(self):
        '''
            Retrieves svg files from LOD Cloud and extracts links to KG metadata.
        '''
        kgs_by_topic = {}

        for dataset in self.lodcloud_data:
            domain = self.lodcloud_data[dataset]['domain']
            if domain == '':
                domain = 'no-domain'
            if domain not in kgs_by_topic:
                kgs_by_topic[domain] = []
            kgs_by_topic[domain].append(self.lodcloud_data[dataset]['identifier'].replace(',',';')) #The replace is useful only to match the identifier from the LODCloud with the KG id in the KGHeartBeat CSV file
            
        for topic in kgs_by_topic:
            print(f"Number of dataset in the topic {topic}: {len(kgs_by_topic[topic])}")
            os.makedirs(f"../data/quality_data/{topic}",exist_ok=True)
        with open('../data/kgs_by_topic.json','w',encoding='utf-8') as file: 
            json.dump(kgs_by_topic, file, indent=4, ensure_ascii=False)    

    def split_kgs_csv_by_topic(self):
        '''
            Extract the KGs from LODCloud and split it by topic in different folder.

            :param dir_path: path to csv where to get the KGs to split by topic.
        '''
        self.recover_lodc_kgs_by_topic()
        with open('../data/kgs_by_topic.json', "r", encoding="utf-8") as file:
            kgs_by_topic_dict = json.load(file)

        for topic in kgs_by_topic_dict:
            kgs_in_the_topic = kgs_by_topic_dict[topic]

            for filename in os.listdir(self.kghb_quality_data_path):
                if '.csv' in filename:
                    file_path = os.path.join(self.kghb_quality_data_path, filename)
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
        for filename in os.listdir(self.kghb_quality_data_path):
                if '.csv' in filename:
                    file_path = os.path.join(self.kghb_quality_data_path, filename)
                    df = pd.read_csv(file_path)

                    df['KG id'] = df['KG id'].astype(str).str.strip()
                    df_filtered = df[~df['KG id'].isin(all_kgs_without_domain)]

                    df_filtered.to_csv(f"../data/quality_data/no-domain/{filename}",index=False)
    
    def extract_only_lodc(self):
        '''
            Extract only KGs from LODCloud from the csv output from KGs Quality Analyzer.

            :param analysis_results_path: path to csv where to discard the KGs.
        '''
        os.makedirs(f"../data/quality_data/all",exist_ok=True)
        identifiers = [data['identifier'].replace(',',';') for key, data in self.lodcloud_data.items()] #The replace is useful only to match the identifier from the LODCloud with the KG id in the KGHeartBeat CSV file
        print(f"Total number of dataset form LOD Cloud: {len(identifiers)}")
        # Iterate throught all the csv and create a new csv with only the KGs from LODCloud
        for filename in os.listdir(self.kghb_quality_data_path):
            if '.csv' in filename:
                file_path = os.path.join(self.kghb_quality_data_path, filename)
                df = pd.read_csv(file_path)

                identifiers_in_csv = set(df['KG id'].unique())
                missing_identifiers = set(identifiers) - identifiers_in_csv

                print(f"File: {file_path} filtered")
                print(f"{len(missing_identifiers)} KGs not analyzed by KGHeartBeat")

                df['KG id'] = df['KG id'].astype(str).str.strip()
                df_filtered = df[df['KG id'].isin(identifiers)]

                df_filtered.to_csv(f"../data/quality_data/all/{filename}",index=False)

split_data = SplitLODCKGsByTopic('../data/quality_data/kghb_output')
split_data.split_kgs_csv_by_topic()
split_data.extract_only_lodc()
