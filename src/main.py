import hydra
import stapy
import logging

from services.df import df_type_conversions
from services.qc import qc_on_df
from services.requests import get_all_datastreams_data
from services.requests import patch_qc_flags


# Type hinting often ignored
# name and COUNT are probably *known* variables names of python property
# might be solved with _name, _count, or NAME, COUNT. when all caps is used, the __str__ will need to be changed to lower
# doesn't work because other ARE with camelcase


# def qc_observation(iot_id: int, function: Callable):
#     log.info(f"start qc {iot_id}")
#     id_list, result_list = get_id_result_lists(iot_id)
#     df_ = pd.DataFrame.from_dict(
#         {Properties.IOT_ID: id_list, "result": result_list}
#     ).astype({Properties.IOT_ID: int, "result": float})
#     return qc_df(df_, function)


# def features_to_global_df(
#     features_dict: dict[int, list[int]], df: pd.DataFrame
# ) -> pd.DataFrame:
#     df_out = df.set_index(Properties.IOT_ID)
#     i = 0
#     for k, v in features_dict.items():
#         log.info(f"{i}/{len(features_dict)}")
#         existing_indices = df_out.index.intersection(v)
#         # df_out.loc[existing_indices] = k
#         i += 1
#     return df_out


log = logging.getLogger(__name__)


# def test_patch_single(id, value):
#     a = Patch.observation(entity_id=id, result_quality=str(value))
#     return a


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    nb_streams_per_call = cfg.data_api.datastreams.top
    top_observations = cfg.data_api.observations.top
    filter_cfg = cfg.data_api.get("filter", {})

    df_all = get_all_datastreams_data(
        thing_id=thing_id,
        nb_streams_per_call=nb_streams_per_call,
        top_observations=top_observations,
        filter_cfg=filter_cfg,
    )

    df_all = qc_on_df(df_all, cfg=cfg)

    df_all = df_type_conversions(df_all)

    url = "http://localhost:8080/FROST-Server/v1.1/$batch"
    counter = patch_qc_flags(df_all, url=url)

    print(f"{counter=}")
    print(f"{df_all.shape=}")
    print(f"{df_all.dtypes=}")

    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
