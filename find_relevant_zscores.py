from datetime import datetime
from functools import partial
import seaborn as sns
import matplotlib.pyplot as plt

from shiny import render, App
from shiny.express import input, ui

import hydra
import pandas as pd
from df_qc_tools.qc import (QCFlagConfig, QualityFlags, calc_gradient_results,
                            calc_zscore_results, get_bool_out_of_range)
from omegaconf import OmegaConf 
from pandassta.df import Df, csv_to_df


def get_date_from_string(
    str_in: str, str_format_in: str = "%Y-%m-%d %H:%M", str_format_out: str = "%Y%m%d"
) -> str:
    date_out = datetime.strptime(str(str_in), str_format_in)
    return date_out.strftime(str_format_out)


df = csv_to_df("testing_dataset.csv")
# df = csv_to_df("SUBSET.csv")
df_subset = df.loc[df[Df.DATASTREAM_ID].isin([7770])]
df_subset[Df.ZSCORE] = calc_zscore_results(df_subset, Df.DATASTREAM_ID, rolling_time_window="10min")[Df.ZSCORE]

ui.input_slider("zscore_limit", "zscore_limit (abs)", 0, 200, 10)

@render.plot(alt="zscore shizzle")
def plot():
    print("testing")

    # global df_subset

    # df_i = df_subset.loc[:]
    # df_i["IN_RANGE"] = True
    # df_i.loc[df_i[Df.ZSCORE].abs() > input.zscore_limit(), "IN_RANGE"] = False

    # df_i = df_subset.sort_values("IN_RANGE")
    ax = sns.scatterplot(data=df_subset, x=Df.TIME, y=Df.RESULT)
    # ax = sns.scatterplot(data=df_i, x=Df.TIME, y=Df.RESULT, hue="IN_RANGE")
    return plt.gca()


# OmegaConf.register_new_resolver("datetime_to_date", get_date_from_string, replace=True)
# 
# 
# @hydra.main(
#     config_path="conf", config_name="config_finetune_config.yaml", version_base="1.2"
# )
# def main(cfg):
#     df = csv_to_df("testing_dataset.csv")
#     df[Df.QC_FLAG] = QualityFlags(0)
# 
#     qc_df = pd.DataFrame.from_dict(cfg.QC, orient="index")
#     ## Is changing this suffusient to correctit?
#     # qc_df.index.name = Df.OBSERVATION_TYPE
#     qc_df.index.name = Df.DATASTREAM_ID
# 
#     ## setup needed columns. Should these be removed?
#     for qc_type in qc_df.keys():
#         qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(
#             qc_type
#         ).apply(pd.Series)
# 
#     df = df.merge(qc_df, on=qc_df.index.name, how="left")
# 
#     df_subset = df.loc[df[Df.DATASTREAM_ID].isin(cfg.get("QC").keys())]
# 
#     # df_subset = calc_gradient_results(df_subset, Df.DATASTREAM_ID)
#     df_subset = calc_zscore_results(df_subset, Df.DATASTREAM_ID, rolling_time_window="120min")
# 
#     ui.input_slider("zscore_limit", "zscore_limit (abs)", 0, 200, 1)
# 
#     @render.plot(alt="zscore shizzle")
#     def plotzscore(df):
#         df["IN_RANGE"] = True
#         df.loc[df[Df.ZSCORE].abs() > input.zscore_limit, "IN_RANGE"] = False
# 
#         df = df.sort_values("IN_RANGE")
#         sns.scatterplot(data=df, x=Df.TIME, y=Df.RESULT, hue="IN_RANGE")
# 
#     # app = App(app_ui, server, debug=True)
# 
# 
#     # qc_flag_config_zscore = QCFlagConfig(
#     #     label="zscore",
#     #     bool_function=partial(get_bool_out_of_range, qc_on=Df.ZSCORE, qc_type="zscore"),
#     #     bool_merge_function=max,
#     #     flag_on_true=QualityFlags.BAD,
#     #     flag_on_false=QualityFlags.PROBABLY_GOOD,
#     #     flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
#     # )
# 
#     # df_subset[Df.QC_FLAG] = qc_flag_config_zscore.execute(df_subset)
# 
#     # df_subset[Df.QC_FLAG] = df_subset[Df.QC_FLAG].astype(str).astype("category")
#     # # ds_0 = df_subset[Df.DATASTREAM_ID].unique()[0] # seabed depth
#     # ds_0 = 7770 # water temperature
#     # df_ds0 = df_subset.loc[df_subset[Df.DATASTREAM_ID] == ds_0]
# 
#     # df_ds0 = df_ds0.sort_values(Df.QC_FLAG)
#     # scp0 = sns.scatterplot(df_ds0, x=Df.TIME, y=Df.RESULT, hue=Df.QC_FLAG)
#     # plt.show()
#     # scp1 = sns.scatterplot(df_ds0.loc[df_ds0[Df.QC_FLAG] == str(QualityFlags(2))], x=Df.TIME, y=Df.RESULT, hue=Df.QC_FLAG)
#     # plt.show()
#     # scp2 = sns.scatterplot(df_ds0.loc[df_ds0[Df.QC_FLAG] == str(QualityFlags(4))], x=Df.TIME, y=Df.RESULT, hue=Df.QC_FLAG)
#     # plt.show()
# 
# 
#     # sns.lineplot()
# 
#     pass
# 
# 
# if __name__ == "__main__":
#     main()
# 