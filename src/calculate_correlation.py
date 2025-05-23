import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
from scipy.stats import pearsonr
from scipy.stats import spearmanr
import os
import numpy as np
from scipy.stats import ttest_ind
import utils
import matplotlib
from matplotlib.colors import TwoSlopeNorm
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt
import seaborn as sns
matplotlib.use('Agg')


here = os.path.dirname(os.path.abspath(__file__))

class CalculateCorrelation:
    def __init__(self, file_path, topic, analysis_result_date):
        '''
            Save the path to the file with the KGs quality data.
            
            :param file_path: path to the file with the KGs data
            :param output_file: name of the file in which to write the results
        '''
        self.analysis_result = file_path
        os.makedirs(f"../data/correlation_results/{topic}",exist_ok=True)
        self.output_file = os.path.join(here,f'../data/correlation_results/{topic}/{analysis_result_date}')


    def calculate_spearman_correlation_matrix(self,columns_to_use, filter_by_ids = False, traditional_dimensions = False, sparql_up = False):
        '''
            Generate the Spearman Correlation matrix by using the values in the columns columns_to_use from the CSV file.      

            :param columns_to_use: list of strings representing the names of the columns from which to take values to measure correlation.
            :param replace_columns: if True, columns that have a list or a boll value as their value will be transformed into a float
        '''
        columns_to_use.append('KG id')
        if traditional_dimensions:
            columns_to_use.append('Sparql endpoint')
        df = pd.read_csv(self.analysis_result,usecols=columns_to_use) 

        if filter_by_ids:
            df = df[df['KG id'].isin(utils.get_always_observed_ids('../data/quality_data/all/2024-01-07.csv'))]
        if traditional_dimensions:
            df.replace('-', np.nan, inplace=True)
        if traditional_dimensions and sparql_up:
            df = df[(df["Sparql endpoint"] == "Available")] 

        # Delete the column to avoid errors
        columns_to_drop = ["KG id","KG name","Sparql endpoint","RDF dump link","Ontology"]
        df = df.drop(columns=columns_to_drop, errors='ignore')

        df.columns = [col.split(' ')[0].split('_')[0] for col in df.columns]
        df.columns = df.columns.str.strip()
        rho = df.corr('spearman')
        pval = df.corr(method=lambda x, y: spearmanr(x, y)[1]) - np.eye(*rho.shape)
        p = pval.map(add_significance_stars)
        
        final_matrix = pd.DataFrame()

        # Iterate through the columns to create pairs of correlation values and stars
        for col in rho.columns:
            # Append correlation values
            final_matrix[col] = rho[col].round(2)  
            # Append corresponding significance stars
            final_matrix[f"{col}_p-value"] = p[col]

        if traditional_dimensions and not sparql_up:
            self.output_file = self.output_file + '_dimensions'
        if traditional_dimensions and sparql_up:
            self.output_file = self.output_file + '_dimensions' + '_sparql_up'

        final_matrix.to_csv(f'{self.output_file}.csv')
        self.draw_heatmap(final_matrix,os.path.basename(self.output_file))


    def draw_heatmap(self, correlation_data, title, replace = False):

        correlation_matrix =  correlation_data.loc[:, ~ correlation_data.columns.str.contains('_p-value')]
        p_value_matrix =  correlation_data.filter(like='_p-value')

        p_value_numeric = p_value_matrix.replace({'\*\*\*': 0.001, '\*\*': 0.01, '\*': 0.05}, regex=True)
        p_value_numeric = p_value_numeric.apply(pd.to_numeric, errors='coerce')

        if replace:
            for col in correlation_matrix:
                    correlation_matrix[col] = correlation_matrix[col].str.replace(',', '.')
                    correlation_matrix[col] = pd.to_numeric(correlation_matrix[col], errors='coerce') 

        masked_correlation_matrix = correlation_matrix.copy()
        masked_correlation_matrix[p_value_numeric > 0.05] = np.nan

        annotations = masked_correlation_matrix.copy().astype(str)
        for i in range(p_value_matrix.shape[0]):
            for j in range(p_value_matrix.shape[1]):
                if not pd.isna(masked_correlation_matrix.iloc[i, j]):
                    if not pd.isna(p_value_matrix.iloc[i, j]):  
                        annotations.iloc[i, j] += f" {p_value_matrix.iloc[i, j]}"

        norm = TwoSlopeNorm(vmin=-1, vcenter=0, vmax=1)
        cmap = LinearSegmentedColormap.from_list('RedGreenRed', ['blue', 'white', 'blue'])

        plt.figure(figsize=(20, 10))
        ax = sns.heatmap(
            masked_correlation_matrix.astype(float), 
            annot=annotations.values,                
            fmt="",                                 
            cmap=cmap,                               
            cbar=True,                               
            norm=norm,                               
            linewidths=0.5,                         
            linecolor="gray"                         
        )

        plt.title(title, fontsize=16)
        ax.set_xticklabels(ax.get_xticklabels(), fontsize=14, rotation=45, ha="right")
        ax.set_yticklabels(ax.get_yticklabels(), fontsize=14)
        plt.tight_layout()
        plt.savefig(self.output_file, dpi=300)
        plt.close()

def add_significance_stars(p):
    if p < 0.001:
        return '***'
    elif p < 0.01:
        return '**'
    elif p < 0.05:
        return '*'
    else:
        return ''  
