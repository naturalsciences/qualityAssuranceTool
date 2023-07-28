from typing import Sequence
import geopandas as gpd
import pandas as pd
import pandas.testing as pdt
import numpy as np
import pytest
from operator import add
from models.enums import QualityFlags
from services.qc import calc_gradient_results, qc_region
from services.regions_query import build_points_query, build_query_points


@pytest.fixture
def points_testing() -> Sequence[Sequence[float]]:
    points = [
        (3.1840709669760137, 51.37115902107277),  # in front of Zeebrugge (North Sea)
        (3.2063475368848096, 51.34661128136423),  # harbor Zeebrugge
        (3.2270907198892886, 51.21143804531792),  # Brugge
        (3.5553432400042584, 56.169369769668116),  # North Sea
    ]

    return points


@pytest.fixture
def df_testing() -> gpd.GeoDataFrame:
    multipl_factor: int = 2
    results_factor: float = 2.345
    base_list_region: list = [
        "NORTH SEA",
        "MAINLAND EUROPE",
        "MAINLAND random",
        None,
        np.nan,
    ]
    base_list_phenomenonTime: list[np.datetime64] = list(
        pd.Timestamp("now")
        + pd.timedelta_range(
            start=0, periods=len(base_list_region), freq="S", unit="s"  # type: ignore
        )
    )
    base_results: list[float] = [
        fi * results_factor for fi in range(len(base_list_region))
    ]
    qc_ref_base = [
        np.NaN,
        QualityFlags.BAD,
        QualityFlags.BAD,
        QualityFlags.PROBABLY_BAD,
        QualityFlags.PROBABLY_BAD,
    ]
    base_observation_type: list[str] = [
        "salinity",
        "Water flow in the scientific seawater circuit",
        "seabed depth",
        "random1",
        "random2",
    ]

    datastream_id_series: pd.Series = pd.Series(
        list(
            sum(  # to convert list of tuples to flat list
                zip(*([list(range(multipl_factor))] * len(base_list_region))), ()
            )
        )
    )
    df_out = gpd.GeoDataFrame(
        {
            "@iot.id": pd.Series(range(len(qc_ref_base) * multipl_factor)),
            "Region": pd.Series(base_list_region * multipl_factor, dtype="string"),
            "qc_ref": pd.Series(qc_ref_base * multipl_factor),
            "datastream_id": pd.Series(
                list(
                    sum(  # to convert list of tuples to flat list
                        zip(*([list(range(multipl_factor))] * len(base_list_region))),
                        (),
                    )
                )
            ),
            "phenomenonTime": pd.Series(base_list_phenomenonTime * multipl_factor),
            "results": pd.Series(
                map(
                    add,
                    base_results * multipl_factor,
                    [i * 10 for i in datastream_id_series.to_list()],
                )
            ),
            "observation_type": pd.Series(
                base_observation_type * multipl_factor, dtype="category"
            ),
        }
    )

    return df_out


def test_build_points_query(points_testing):
    q: str = build_points_query(points_testing)
    substr_count = q.count("ST_SetSRID(ST_MakePoint(")
    srid_count = q.count("), 4326))")
    assert substr_count == len(points_testing) and srid_count == len(points_testing)


@pytest.mark.skip(reason="bit ridiculous to test")
def test_build_query_seavox():
    assert 0


@pytest.mark.skip(reason="is response dependency")
def test_location_north_sea():
    assert 0


@pytest.mark.skip(reason="is response dependency")
def test_location_mainland_eu():
    assert 0


def test_qc_region_to_flag(df_testing):
    df_out = qc_region(df_testing)
    pdt.assert_series_equal(
        df_out.qc_flag.fillna("nan").astype("string"),
        df_out.qc_ref.fillna("nan").astype("string"),
        check_names=False,
    )


def test_qc_gradient_cacl_zero(df_testing):
    df_testing["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
        start=0, periods=df_testing.shape[0], freq="S", unit="s"  # type: ignore
    )
    df_testing["result"] = 1.0
    df = calc_gradient_results(df_testing, "datastream_id")
    pdt.assert_series_equal(
        df.gradient, pd.Series(np.zeros_like(df_testing.result), name="gradient")
    )


def test_qc_gradient_cacl_one(df_testing):
    df_testing["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
        start=0, periods=df_testing.shape[0], freq="S", unit="s"  # type: ignore
    )
    df_testing["result"] = pd.Series(range(df_testing.shape[0]), dtype="float")
    df = calc_gradient_results(df_testing, "datastream_id")
    pdt.assert_series_equal(
        df.gradient, pd.Series(np.ones_like(df_testing.result), name="gradient")
    )


def test_qc_gradient_cacl_neg_one(df_testing):
    df_testing["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
        start=0, periods=df_testing.shape[0], freq="S", unit="s"  # type: ignore
    )
    df_testing["result"] = pd.Series(range(df_testing.shape[0], 0, -1), dtype="float")
    df = calc_gradient_results(df_testing, "datastream_id")
    pdt.assert_series_equal(
        df.gradient, pd.Series(np.ones_like(df_testing.result) * -1, name="gradient")
    )


def test_qc_gradient_cacl_vardt_pos(df_testing):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        ) * list(range(df_slice.shape[0]))
        df_slice["result"] = pd.Series(range(df_slice.shape[0]), dtype="float")
        df = calc_gradient_results(df_slice, "datastream_id")
        pdt.assert_series_equal(
            df.gradient,
            pd.Series(
                np.gradient(
                    df_slice.result, [(1 * i**2) for i in range(df_slice.shape[0])]
                ),
                name="gradient",
            ),
            check_index=False,
        )


def test_qc_gradient_cacl_vardt_neg(df_testing):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        ) * list(range(df_slice.shape[0]))
        df_slice["result"] = pd.Series(range(df_slice.shape[0], 0, -1), dtype="float")
        df = calc_gradient_results(df_slice, "datastream_id")
        pdt.assert_series_equal(
            df.gradient,
            pd.Series(
                np.gradient(
                    df_slice.result, [(1 * i**2) for i in range(df_slice.shape[0])]
                ),
                name="gradient",
            ),
            check_index=False,
        )


def test_qc_gradient_cacl_vardx_pos(df_testing):
    def grad_cte_dt(fm1, fp1, dh):
        return (fp1 - fm1) / (2.0 * dh)

    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        )

        df_slice["result"] = pd.Series(
            np.array(range(df_slice.shape[0])) * range(df_slice.shape[0]), dtype="float"
        )
        df = calc_gradient_results(df_slice, "datastream_id")

        grad_ref = grad_cte_dt(
            df_slice.result.shift().interpolate(
                method="slinear", limit_direction="both", fill_value="extrapolate"
            ),
            df_slice.result.shift(-1).interpolate(
                method="slinear", limit_direction="both", fill_value="extrapolate"
            ),
            dh=1.0,
        )

        pdt.assert_series_equal(
            df.gradient,
            grad_ref,
            check_index=False,
            check_names=False,
        )


def test_qc_gradient_cacl_vardx_neg(df_testing):
    def grad_cte_dt(fm1, fp1, dh):
        return (fp1 - fm1) / (2.0 * dh)

    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        )

        df_slice["result"] = pd.Series(
            np.array(range(df_slice.shape[0], 0, -1)) * range(df_slice.shape[0]),
            dtype="float",
        )
        df = calc_gradient_results(df_slice, "datastream_id")

        grad_ref = grad_cte_dt(
            df_slice.result.shift().interpolate(
                method="slinear", limit_direction="both", fill_value="extrapolate"
            ),
            df_slice.result.shift(-1).interpolate(
                method="slinear", limit_direction="both", fill_value="extrapolate"
            ),
            dh=1.0,
        )

        pdt.assert_series_equal(
            df.gradient,
            grad_ref,
            check_index=False,
            check_names=False,
        )


def test_example_pivot_and_reverse():
    df = pd.DataFrame(
        {
            "type": [0, 0, 1, 1],
            "time": [1.1, 2.2, 1.1, 2.2],
            "result": list(range(4)),
            "flag": [str(i) for i in range(4)],
        }
    )
    df_p = df.pivot(index=["time"], columns=["type"], values=["result", "flag"])
    df_p_undone = (
        df_p.stack()
        .reset_index()
        .sort_values("type")
        .reset_index(drop=True)
        .sort_index(axis=1)
    )
    df_p_undone.result = df_p_undone.result.astype(int)
    pdt.assert_frame_equal(df_p_undone.sort_index(axis=1), df.sort_index(axis=1))


# @pytest.mark.skip()
def test_qc_dependent_quantities(df_testing):
    qc_flag_count_ref = {"0": df_testing.shape[0]-2, "4":2}
    df_testing["qc_flag"] = "0"

    idx_ = df_testing.loc[df_testing["datastream_id"] == 0].index[2]
    df_testing.loc[idx_, "qc_flag"] = "4"
    df_pivot = df_testing.pivot(
        index=["phenomenonTime"],
        columns=["datastream_id"],
        values=["results", "qc_flag", "observation_type", "@iot.id"],
    )
    mask = (~df_pivot["qc_flag", 0].isin(["0", "1", "2"]))
    df_pivot.loc[mask, ("qc_flag", 1)] = df_pivot.loc[mask, ("qc_flag", 0)]

    df_unpivot = df_pivot.stack().reset_index().set_index("@iot.id")
    df_testing = df_testing.set_index("@iot.id")
    df_testing.loc[df_unpivot.index, "qc_flag"] = df_unpivot["qc_flag"]
    assert df_testing.qc_flag.value_counts().to_dict() == qc_flag_count_ref
