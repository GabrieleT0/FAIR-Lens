import pandas as pd
import utils 

class EvaluateFAIRness:

    def __init__(self,quality_data_to_evaluate, output_file_path):
        self.quality_data = pd.read_csv(quality_data_to_evaluate)
        self.output_file_path = output_file_path
        self.fairness_evaluation = self.initialize_output_file()
            
    def evaluate_findability(self):
        
        doi_indication = self.quality_data['KG id'].apply(utils.recover_doi_from_lodcloud)
        self.fairness_evaluation["F1-M Unique and persistent ID"] = doi_indication

        self.fairness_evaluation["F1-D URIs dereferenceability"] = (pd.to_numeric(self.quality_data['URIs Deferenceability'], errors='coerce').fillna('-'))
        self.fairness_evaluation["F1-D URIs dereferenceability"] = self.fairness_evaluation["F1-D URIs dereferenceability"].apply(lambda x: x if x != '-' else 0)

        self.fairness_evaluation["F2a-M - Metadata availability via standard primary sources"] = 1 #Dataset is at least in the LOD Cloud

        sparql_indication = self.quality_data["SPARQL endpoint URL"].apply(lambda x: 1 if pd.notna(x) and x != '' else 0)
        doi_indication = self.quality_data['KG id'].apply(utils.recover_doi_from_lodcloud)
        dump_indication = self.quality_data["Availability of RDF dump (metadata)"].apply(lambda x: 1 if x in [1,"1"] else 0)
        verifiability_info = self.quality_data.apply(utils.check_publisher_info,axis=1)
        mediatype_indication = self.quality_data['metadata-media-type'].apply(lambda x: 1 if x not in ('[]','['',]',"['']") else 0)

        license = self.quality_data['License machine redeable (metadata)'].apply(lambda x: 1 if x not in ['-', '',False,'False'] and pd.notna(x) else 0)
        license_query = self.quality_data['License machine redeable (query)'].apply(lambda x: 1 if x not in ['-', '',False,'False','[]'] and pd.notna(x) else 0)
        license_value = (license | license_query).astype(int)
        vocabs =  self.quality_data['Vocabularies'].apply(lambda x: 1 if x not in ['[]','-'] and pd.notna(x) else 0)      
        links = self.quality_data.apply(
            lambda row: (
                1 if (
                    (row['Degree of connection'] != '-' and pd.notna(row['Degree of connection']) and int(row['Degree of connection']) > 0)
                    or (row['Number of samAs chains'] not in ['-', 0, '0'] and pd.notna(row['Number of samAs chains']) and int(row['Number of samAs chains']) > 0)
                    or (row['SKOS mapping properties'] not in ['-', 0, '0'] and pd.notna(row['SKOS mapping properties']) and int(row['SKOS mapping properties']) > 0) 
                ) else 0
            ), axis=1
        )
        void_indication = self.quality_data['Url file VoID'].apply(lambda x: 1 if pd.notna(x) and x != '' else 0)
        self.fairness_evaluation["F2b-M Metadata availability for all the attributes covered in the FAIR score computation"] = ((sparql_indication + doi_indication + dump_indication + verifiability_info + mediatype_indication + license_value + vocabs + links + void_indication) / 9).round(2)

        self.fairness_evaluation["F3-M Data referrable via a DOI"] = self.quality_data['KG id'].apply(utils.recover_doi_from_lodcloud)

        self.fairness_evaluation["F4-M Metadata registered in a searchable engine"] = 1 # Dataset is at least in the LOD Cloud

        self.fairness_evaluation["F score"] = (self.fairness_evaluation[["F1-M Unique and persistent ID", "F1-D URIs dereferenceability", "F2a-M - Metadata availability via standard primary sources", "F2b-M Metadata availability for all the attributes covered in the FAIR score computation", "F3-M Data referrable via a DOI", "F4-M Metadata registered in a searchable engine"]].sum(axis=1) / 6).round(2)
        print("Findability evaluation completed!")


    def evaluate_availability(self):

        sparql_availability = self.quality_data["Sparql endpoint"].apply(lambda x: 1 if x == 'Available' else 0)
        dump_availability = self.quality_data["Availability of RDF dump (metadata)"].apply(lambda x: 1 if x in [1,"1"] else 0) # No consideration about the mediatype of the available dump
        sparql_on_not_interop = self.quality_data["SPARQL endpoint URL"].apply(utils.check_at_least_sparql_on)
        sparql_or_dump_on = (
            (sparql_availability == 1) | (dump_availability == 1)
        ).astype(int)

        self.fairness_evaluation["A1-D Working access point(s)"] = sparql_or_dump_on.combine(
            sparql_on_not_interop,
            lambda x, y: 1 if x == 1 else (0.5 if x == 0 and y == 1 else 0)
        )

        self.fairness_evaluation["A1-M Metadata availability via working primary sources"] = 1 # Metadata is at available at least in the LOD Cloud

        self.fairness_evaluation["A1.2 Authentication & HTTPS support"] = self.quality_data.apply(
            lambda row: ((1 if (row["Use HTTPS"] in ['True',True] or row["Sparql endpoint"] == 'Available') else 0) + 
                        (1 if row["Requires authentication"] in ["False", False, True, 'True'] else 0)) / 2,
            axis=1
        )

        self.fairness_evaluation["A2-M Registered in search engines"] = 1  # Metadata is registered at least in the LOD Cloud
        
        self.fairness_evaluation["A score"] = (self.fairness_evaluation[["A1-D Working access point(s)", "A1-M Metadata availability via working primary sources", "A1.2 Authentication & HTTPS support", "A2-M Registered in search engines"]].sum(axis=1) / 4).round(2)
        print("Availability evaluation completed!")
    
    def evaluate_reusability(self):

        self.fairness_evaluation['R1.1 Machine- or human-readable license retrievable via any primary source'] = self.quality_data.apply(
            lambda row: 1 if (
                pd.notna(row['License machine redeable (metadata)']) and row['License machine redeable (metadata)'] not in ['-', '',False,'False']
            ) or (
                pd.notna(row['License machine redeable (query)']) and row['License machine redeable (query)'] not in ['-', ''] 
            ) or (
                row['License human redeable'] in [True, 'True']
            ) else 0,
            axis=1
        )

        self.fairness_evaluation['R1.2 Publisher information, such as authors, contributors, publishers, and sources'] = self.quality_data.apply(utils.check_publisher_info,axis=1)
        
        # If the media type is is standard and open (SW standard), in this community also this format is common accepted. If not, we have to check if the data are in a standard format only for the community
        self.fairness_evaluation['R1.3-D Data organized in a standardized way'] = self.quality_data.apply(
            lambda row: 1 if row['Availability of a common accepted Media Type'] in ['True', True] 
            else (1 if 'api/sparql' or 'rdf' in row['metadata-media-type'] else row['metadata-media-type']),
            axis=1
        )

        metadata_in_sparql = self.quality_data['SPARQL endpoint URL'].apply(utils.check_meta_in_sparql)
        self.fairness_evaluation['R1.3-M Metadata are described with VoID/DCAT predicates'] = (
            (self.quality_data['metadata-media-type'].str.contains('meta/void', na=False).astype(int) | 
            (metadata_in_sparql == 1)).astype(int) | 
            (~self.quality_data['License machine redeable (query)'].isin(['-','',False,'False'])).astype(int)
        )
        self.fairness_evaluation["R score"] = (self.fairness_evaluation[["R1.1 Machine- or human-readable license retrievable via any primary source", "R1.2 Publisher information, such as authors, contributors, publishers, and sources", "R1.3-D Data organized in a standardized way", "R1.3-M Metadata are described with VoID/DCAT predicates"]].sum(axis=1) / 4).round(2)

        print("Reusability evaluation completed!")

    def evaluate_interoperability(self):

        self.fairness_evaluation['I1-D Standard & open representation format'] = self.quality_data.apply(
            lambda row: 1 if row['Availability of a common accepted Media Type'] in ['True', True] 
            else (1 if 'api/sparql' or 'rdf' in row['metadata-media-type'].lower() else 0),
            axis=1
        )
        
        metadata_in_sparql = self.quality_data['SPARQL endpoint URL'].apply(utils.check_meta_in_sparql)
        self.fairness_evaluation['I1-M Metadata are described with VoID/DCAT predicates'] = (
            (self.quality_data['metadata-media-type'].str.contains('meta/void', na=False).astype(int) | 
            (metadata_in_sparql == 1)).astype(int) | 
            (~self.quality_data['License machine redeable (query)'].isin(['-','',False,'False'])).astype(int)
        )

        self.fairness_evaluation['I2 Use of FAIR vocabularies'] = (self.quality_data['Vocabularies'].apply(utils.check_if_fair_vocabs)).round(2)

        
        self.fairness_evaluation['I3-D Degree of connection'] = self.quality_data.apply(
            lambda row: (
                1 if (
                    (row['Degree of connection'] != '-' and pd.notna(row['Degree of connection']) and int(row['Degree of connection']) > 0)
                    or (row['Number of samAs chains'] not in ['-', 0, '0'] and pd.notna(row['Number of samAs chains']) and int(row['Number of samAs chains']) > 0)
                    or (row['SKOS mapping properties'] not in ['-', 0, '0'] and pd.notna(row['SKOS mapping properties']) and int(row['SKOS mapping properties']) > 0) 
                ) else 0
            ), axis=1
        )
        self.fairness_evaluation['I score'] = (self.fairness_evaluation[["I1-D Standard & open representation format", "I1-M Metadata are described with VoID/DCAT predicates", "I2 Use of FAIR vocabularies", "I3-D Degree of connection"]].sum(axis=1).round(2) / 4).round(2)
        print("Interoperability evaluation completed!")

    def calculate_FAIR_score(self):
        self.fairness_evaluation["FAIR score"] = self.fairness_evaluation[["F score", "A score", "I score", "R score"]].sum(axis=1).round(2)

    def initialize_output_file(self):
        output_df = pd.DataFrame({
            "KG id": self.quality_data["KG id"],             
            "KG name": self.quality_data["KG name"], 
            "KG SPARQL endpoint": self.quality_data['SPARQL endpoint URL'],
            "RDF dump link" : self.quality_data["URL for download the dataset"],
            "Ontology": self.quality_data['KG id'].apply(utils.check_if_ontology)
        })

        return output_df

    def save_file(self):
        self.fairness_evaluation.to_csv(self.output_file_path,index=False)
    