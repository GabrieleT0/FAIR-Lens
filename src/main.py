from split_data_by_topic import SplitLODCKGsByTopic
from evaluate_fairness import EvaluateFAIRness
from calculate_correlation import CalculateCorrelation
from generate_boxplots import GenerateBoxplots
import json
import glob
import os
import utils

# Split KGHB quality data into quality data separated by topic
def split_quality_data_by_domain():
    split_data = SplitLODCKGsByTopic('../data/quality_data/kghb_output')
    split_data.split_kgs_csv_by_topic()
    split_data.extract_only_lodc()

# Verify the normal distribution of the FAIRness evaluation data (if only one column is not normal distributed, we can't use Pearson correlation but Spearman)
def verify_normal_distribution(kgs_by_topic):
   
    for topic in kgs_by_topic:
        print(f"Verifying normal distribution for subclouds {topic}...")
        utils.verify_normal_distribution(f'../data/fairness_evaluation_results/{topic}/2025-04-14.csv',['F1-M Unique and persistent ID','F1-D URIs dereferenceability','F2a-M - Metadata availability via standard primary sources',
                                                                                                'F2b-M Metadata availability for all the attributes covered in the FAIR score computation','F3-M Data referrable via a DOI',
                                                                                                'F4-M Metadata registered in a searchable engine','F score','A1-D Working access point(s)','A1-M Metadata availability via working primary sources',
                                                                                                'A1.2 Authentication & HTTPS support','A2-M Registered in search engines','A score','I1-D Standard & open representation format',
                                                                                                'I1-M Metadata are described with VoID/DCAT predicates','I2 Use of FAIR vocabularies','I3-D Degree of connection','I score',
                                                                                                'R1.1 Machine- or human-readable license retrievable via any primary source',"R1.2 Publisher information, such as authors, contributors, publishers, and sources",
                                                                                                'R1.3-D Data organized in a standardized way','R1.3-M Metadata are described with VoID/DCAT predicates','R score','FAIR score'])
     
# Calculate the FAIRness by topic
def evaluate_fairness(kgs_by_topic):

    for topic in kgs_by_topic:
        print(f"Evaluating the FAIRness of the {topic} subcloud")
        csv_files = glob.glob(os.path.join(f'../data/quality_data/{topic}/', '*.csv'))
        os.makedirs(f"../data/fairness_evaluation_results/{topic}",exist_ok=True)
        for csv_file in csv_files:
            fairness = EvaluateFAIRness(csv_file,f"../data/fairness_evaluation_results/{topic}/{os.path.basename(csv_file)}")
            fairness.evaluate_findability()
            fairness.evaluate_availability()
            fairness.evaluate_interoperability()
            fairness.evaluate_reusability()
            fairness.calculate_FAIR_score()
            fairness.save_file()

def calculate_correlation(kgs_by_topic):

    for topic in kgs_by_topic:
        csv_files = glob.glob(os.path.join(f'../data/fairness_evaluation_results/{topic}/', '*.csv'))
        for csv_file in csv_files:
            correlation = CalculateCorrelation(f'../data/fairness_evaluation_results/{topic}/{os.path.basename(csv_file)}',topic,os.path.basename(csv_file).split('.')[0]) # Only one file we have for now (only one observation)
            correlation.calculate_spearman_correlation_matrix(
                ['F1-M Unique and persistent ID','F1-D URIs dereferenceability','F2a-M - Metadata availability via standard primary sources',
                                                                                                        'F2b-M Metadata availability for all the attributes covered in the FAIR score computation','F3-M Data referrable via a DOI',
                                                                                                        'F4-M Metadata registered in a searchable engine','F score','A1-D Working access point(s)','A1-M Metadata availability via working primary sources',
                                                                                                        'A1.2 Authentication & HTTPS support','A2-M Registered in search engines','A score','I1-D Standard & open representation format',
                                                                                                        'I1-M Metadata are described with VoID/DCAT predicates','I2 Use of FAIR vocabularies','I3-D Degree of connection','I score',
                                                                                                        'R1.1 Machine- or human-readable license retrievable via any primary source',"R1.2 Publisher information, such as authors, contributors, publishers, and sources",
                                                                                                        'R1.3-D Data organized in a standardized way','R1.3-M Metadata are described with VoID/DCAT predicates','R score','FAIR score'],
            True)

def generate_boxplots():

    fair_score_boxplot = GenerateBoxplots('../data/fairness_evaluation_results')

    fair_score_boxplot.generate_combined_boxplot('../charts','F score',0,1.01,True)
    fair_score_boxplot.generate_combined_boxplot('../charts','A score',0,1.01,True)
    fair_score_boxplot.generate_combined_boxplot('../charts','I score',0,1.01,True)
    fair_score_boxplot.generate_combined_boxplot('../charts','R score',0,1.01,True)
    fair_score_boxplot.generate_combined_boxplot('../charts','FAIR score',0,4,True)

if __name__ == "__main__":
    with open('../data/kgs_by_topic.json', "r", encoding="utf-8") as f:
        kgs_by_topic = json.load(f)
    kgs_by_topic['all'] = [] # Only useful to evaluate the FAIRness on the entire LOD Cloud, to use it as baseline (no topical distinction)
    
    split_quality_data_by_domain()
    evaluate_fairness(kgs_by_topic)
    verify_normal_distribution(kgs_by_topic)
    calculate_correlation(kgs_by_topic)