from pandas import CategoricalDtype
import seaborn as sns
from palmerpenguins import load_penguins
from shiny import render
from shiny.express import input, ui
from df_qc_tools.qc import (QCFlagConfig, QualityFlags, calc_gradient_results,
                            calc_zscore_results, get_bool_out_of_range)
from pandassta.df import Df, csv_to_df
import matplotlib.pyplot as plt

penguins = load_penguins()

df = csv_to_df("SUBSET.csv").sort_values(Df.TIME)
df = csv_to_df("testing_dataset.csv").sort_values(Df.TIME)

df = calc_zscore_results(df, Df.DATASTREAM_ID, rolling_time_window="60min")
df = df.loc[df[Df.DATASTREAM_ID] == 7770]
df["RANGE"] = 0

#SET CATEGORY RANGE

ui.input_slider("zscore_abs_limit", "absolute zscore limit", 1, 100, 20)
ui.input_slider("max", "max", df[Df.RESULT].min(), df[Df.RESULT].max()*1.1, df[Df.RESULT].max()*1.1)
ui.input_slider("min", "min", df[Df.RESULT].min(), df[Df.RESULT].max()*1.1, df[Df.RESULT].min()*1.1)

@render.plot(alt="Results plot with zscore filter indication")  
def plot():  
    # ax = sns.scatterplot(data=penguins, x="body_mass_g", y="bill_length_mm")  
    df["RANGE"] = 0
    df.loc[df[Df.ZSCORE].abs() > input.zscore_abs_limit(), "RANGE"] = 1
    plt.scatter(df[Df.TIME].values, df[Df.RESULT].values, c="b", s=1)
    df_one = df.loc[df["RANGE"] == 1]
    plt.scatter(df_one[Df.TIME].values, df_one[Df.RESULT].values, c="r")
    ax = plt.gca() 
    ax.set_ylim([input.min(), input.max()])
    # ax = sns.scatterplot(data=df, x=Df.TIME, y=Df.RESULT, hue="RANGE", linewidth=0)  
    # ax.set_title("Palmer Penguins")
    # ax.set_xlabel("Mass (g)")
    # ax.set_ylabel("Count")
    return ax