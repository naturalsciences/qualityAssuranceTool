from pandas import CategoricalDtype
import seaborn as sns
from shiny import render
from shiny.express import input, ui
from df_qc_tools.qc import (QCFlagConfig, QualityFlags, calc_gradient_results,
                            calc_zscore_results, get_bool_out_of_range)
from pandassta.df import Df, csv_to_df
import matplotlib.pyplot as plt


# df = csv_to_df("SUBSET.csv").sort_values(Df.TIME)
df = csv_to_df("testing_dataset.csv").sort_values(Df.TIME)

df.loc[(df[Df.DATASTREAM_ID] == 7795) & (df[Df.RESULT] <= 0.75), Df.QC_FLAG] = QualityFlags(3) 
df = calc_zscore_results(df, Df.DATASTREAM_ID, rolling_time_window="600min")

# df = df.loc[df[Df.DATASTREAM_ID] == 7770]
# df = df.loc[df[Df.DATASTREAM_ID] == 7850]
df = df.loc[df[Df.DATASTREAM_ID].isin([7849, 7795])]
# df = df.drop(df.loc[df[Df.RESULT] <= 10.].index)
df = df.sort_values(Df.ZSCORE)
df["RANGE"] = 0

min_r2 = round(df.loc[df[Df.DATASTREAM_ID] == 7849, Df.RESULT].min(), 2)
max_r2 = round(df.loc[df[Df.DATASTREAM_ID] == 7849, Df.RESULT].max(), 2)


#SET CATEGORY RANGE

ui.input_slider("zscore_abs_limit", "absolute zscore limit", 1, 100, 20)
ui.input_slider("max", "max", round(min_r2*0.9, 2), round(max_r2*1.1, 2), round(max_r2*1.1, 2))
ui.input_slider("min", "min", round(min_r2*0.9, 2), round(max_r2*1.1, 2), round(min_r2*0.9, 2))

@render.plot(alt="Results plot with zscore filter indication")  
def plot():  
    # ax = sns.scatterplot(data=penguins, x="body_mass_g", y="bill_length_mm")  
    df["RANGE"] = 0
    df.loc[df[Df.ZSCORE].abs() > input.zscore_abs_limit(), "RANGE"] = 1
    # plt.scatter(df[Df.TIME].values, df[Df.RESULT].values, c="b", s=1)
    # df_one = df.loc[df["RANGE"] == 1]
    # plt.scatter(df_one[Df.TIME].values, df_one[Df.RESULT].values, c="r")
    # ax = plt.gca() 
    # ax.set_ylim([input.min(), input.max()])
    fig, ax = plt.subplots(1, 2)
    sns.scatterplot(data=df.loc[df[Df.DATASTREAM_ID] == 7849], x=Df.TIME, y=Df.RESULT, hue="RANGE", linewidth=0, ax=ax[0], legend=False)  
    sns.scatterplot(data=df.loc[df[Df.DATASTREAM_ID] == 7795], x=Df.TIME, y=Df.RESULT, hue="RANGE", linewidth=0, ax=ax[1], legend=False)  
    # ax = sns.scatterplot(data=df, x=Df.TIME, y=Df.RESULT, hue="RANGE", linewidth=0)  
    # ax.set_title("Palmer Penguins")
    # ax.set_xlabel("Mass (g)")
    # ax.set_ylabel("Count")
    return fig