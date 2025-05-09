from split_data_by_topic import SplitLODCKGsByTopic
from evaluate_fairness import EvaluateFAIRness
import json
import glob
import os

# Split KGHB quality data into quality data separated by topic
def split_quality_data_by_domain():
    split_data = SplitLODCKGsByTopic('../data/quality_data/kghb_output')
    split_data.split_kgs_csv_by_topic()
    split_data.extract_only_lodc()

# Calculate the FAIRness by topic
def evaluate_fairness():

    with open('../data/kgs_by_topic.json', "r", encoding="utf-8") as f:
        kgs_by_topic = json.load(f)

    kgs_by_topic['all'] = [] # Only useful to evaluate the FAIRness on the entire LOD Cloud, to use it as baseline (no topical distinction)

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