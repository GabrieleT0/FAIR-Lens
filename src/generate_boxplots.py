import glob
import os
import json
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

class GenerateBoxplots():
    def __init__(self,fariness_evaluation_path):
        with open('../data/kgs_by_topic.json', "r", encoding="utf-8") as f:
            kgs_by_topic = json.load(f)
        self.csv_files = []
        for topic in kgs_by_topic:
            self.csv_files.append((topic,glob.glob(os.path.join(f'{fariness_evaluation_path}/{topic}/', '*.csv')))) 
    
    def generate_combined_boxplot(self,output_dir,column_to_plot,y_min,y_max):
        fair_scores = []

        for label, file in self.csv_files:
            if len(file) > 0:
                df = pd.read_csv(file[0]) # This beacuase for now we only look to the last analysis and not over time
                if column_to_plot in df.columns:
                    fair_scores.append(pd.DataFrame({
                        column_to_plot : df[column_to_plot],
                        'Subclouds': label
                    }))
                else:
                    print(f"Warning: {column_to_plot} column not found in {file}")

        combined_df = pd.concat(fair_scores, ignore_index=True)

        plt.figure(figsize=(12, 6))
        sns.boxplot(data=combined_df, x='Subclouds', y=f'{column_to_plot}')
        plt.xticks(rotation=45)
        plt.ylim(y_min,y_max)
        plt.title(f'{column_to_plot} distribution across subclouds')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/{column_to_plot}')
