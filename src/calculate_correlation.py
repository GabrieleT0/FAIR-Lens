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


    def calculate_spearman_correlation_matrix(self, columns_to_use, filter_by_ids=False, traditional_dimensions=False, sparql_up=False,ci_level=95, n_bootstrap=1000):
        """
        Generate the Spearman correlation matrix with CI and significance.
        """
        columns_to_use.append('KG id')
        if traditional_dimensions:
            columns_to_use.append('Sparql endpoint')
        df = pd.read_csv(self.analysis_result, usecols=columns_to_use)

        if filter_by_ids:
            import utils
            df = df[df['KG id'].isin(utils.get_always_observed_ids('../data/quality_data/all/2024-01-07.csv'))]
        if traditional_dimensions:
            df.replace('-', np.nan, inplace=True)
        if traditional_dimensions and sparql_up:
            df = df[(df["Sparql endpoint"] == "Available")]

        # Drop unused cols
        columns_to_drop = ["KG id", "KG name", "Sparql endpoint", "RDF dump link", "Ontology"]
        df = df.drop(columns=columns_to_drop, errors='ignore')

        df.columns = [col.split(' ')[0].split('_')[0] for col in df.columns]
        df.columns = df.columns.str.strip()

        # Prepare matrices
        correlation_numeric = pd.DataFrame(index=df.columns, columns=df.columns, dtype=float)
        annotation_matrix = pd.DataFrame(index=df.columns, columns=df.columns, dtype=object)

        for i, col1 in enumerate(df.columns):
            for j, col2 in enumerate(df.columns):
                if j < i:
                    continue
                rho, (ci_low, ci_high) = spearman_ci(df[col1], df[col2], ci=ci_level, n_bootstrap=n_bootstrap)
                _, p = spearmanr(df[col1], df[col2])
                
                # numeric for heatmap colors
                correlation_numeric.loc[col1, col2] = rho
                correlation_numeric.loc[col2, col1] = rho

                # annotation string including r^2
                annotation_val = f"{rho:.2f} {add_significance_stars(p)} CI=[{ci_low:.2f}, {ci_high:.2f}]"
                annotation_matrix.loc[col1, col2] = annotation_val
                annotation_matrix.loc[col2, col1] = annotation_val

        if traditional_dimensions and not sparql_up:
            self.output_file = self.output_file + '_dimensions'
        if traditional_dimensions and sparql_up:
            self.output_file = self.output_file + '_dimensions' + '_sparql_up'

        # save csv with annotations
        annotation_matrix.to_csv(f'{self.output_file}.csv')

        # draw heatmap
        self.draw_heatmap(correlation_numeric, annotation_matrix, os.path.basename(self.output_file))


    def draw_heatmap(self, correlation_numeric, annotation_matrix, title):
        """
        Draw heatmap with numeric values for color and annotations with rho + stars only.
        """
        # Build simplified annotation matrix: just rho + stars
        simple_annotations = correlation_numeric.copy().astype(str)
        for i in range(annotation_matrix.shape[0]):
            for j in range(annotation_matrix.shape[1]):
                if pd.notna(correlation_numeric.iloc[i, j]):
                    rho_val = correlation_numeric.iloc[i, j]
                    # Extract stars from full annotation string
                    stars = ""
                    ann_str = annotation_matrix.iloc[i, j]
                    if ann_str and isinstance(ann_str, str):
                        if "***" in ann_str:
                            stars = "***"
                        elif "**" in ann_str:
                            stars = "**"
                        elif "*" in ann_str:
                            stars = "*"
                    simple_annotations.iloc[i, j] = f"{rho_val:.2f} {stars}"
                else:
                    simple_annotations.iloc[i, j] = ""

        # Heatmap colors still based on numeric rho
        norm = TwoSlopeNorm(vmin=-1, vcenter=0, vmax=1)
        cmap = LinearSegmentedColormap.from_list('BlueWhiteBlue', ['blue', 'white', 'blue'])

        plt.figure(figsize=(20, 10))
        ax = sns.heatmap(
            correlation_numeric.astype(float),
            annot=simple_annotations.values,
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

def spearman_ci(x, y, n_bootstrap=1000, ci=95, random_state=None):
    """
    Compute Spearman correlation with bootstrap confidence interval.
    
    :param x: first variable (array-like)
    :param y: second variable (array-like)
    :param n_bootstrap: number of bootstrap resamples
    :param ci: confidence level (e.g., 95)
    :return: rho, (ci_low, ci_high)
    """
    rng = np.random.default_rng(random_state)
    x, y = np.array(x), np.array(y)
    mask = ~np.isnan(x) & ~np.isnan(y)   # remove NaNs
    x, y = x[mask], y[mask]

    if len(x) < 3:  # not enough data
        return np.nan, (np.nan, np.nan)

    # observed Spearman correlation
    rho, _ = spearmanr(x, y)

    # bootstrap resampling
    bootstrapped = []
    for _ in range(n_bootstrap):
        idx = rng.integers(0, len(x), len(x))
        rho_b, _ = spearmanr(x[idx], y[idx])
        bootstrapped.append(rho_b)

    lower = np.percentile(bootstrapped, (100-ci)/2)
    upper = np.percentile(bootstrapped, 100-(100-ci)/2)

    return rho, (lower, upper)

def add_significance_stars(p):
    if p < 0.001:
        return '***'
    elif p < 0.01:
        return '**'
    elif p < 0.05:
        return '*'
    else:
        return ''  
