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
        kgs_by_topic['all'] = []
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

        summary = combined_df.groupby('Subclouds')[column_to_plot].describe()
        outliers_df = self.get_outliers(combined_df, value_column='R score', category_column='Subclouds')
        print(summary)
        outliers_df.to_csv('outliers.csv', index=False)

        plt.figure(figsize=(12, 6))
        sns.boxplot(data=combined_df, x='Subclouds', y=f'{column_to_plot}')
        plt.xticks(rotation=45)
        plt.ylim(y_min,y_max)
        plt.title(f'{column_to_plot} distribution across subclouds')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/{column_to_plot}')


    def get_outliers(self, df, value_column, category_column):
        outliers = []
        grouped = df.groupby(category_column)

        for category, group in grouped:
            q1 = group[value_column].quantile(0.25)
            q3 = group[value_column].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            category_outliers = group[(group[value_column] < lower_bound) | (group[value_column] > upper_bound)]
            outliers.append(category_outliers)

        return pd.concat(outliers)

    
test = GenerateBoxplots('../data/fairness_evaluation_results')
test.generate_combined_boxplot('../charts','R score')