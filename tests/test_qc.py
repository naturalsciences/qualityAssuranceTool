from typing import Sequence
import geopandas as gpd
import pandas as pd
import pandas.testing as pdt
import numpy as np
import pytest
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
    base_list_region: list = [
        "NORTH SEA",
        "MAINLAND EUROPE",
        "MAINLAND random",
        None,
        np.nan,
    ]

    df_out = gpd.GeoDataFrame(
        {
            "Region": pd.Series(base_list_region * multipl_factor, dtype="string"),
            "qc_ref": pd.Series(
                [
                    np.NaN,
                    QualityFlags.BAD,
                    QualityFlags.BAD,
                    QualityFlags.PROBABLY_BAD,
                    QualityFlags.PROBABLY_BAD,
                ]
                * multipl_factor,
            ),
            "datastream_id": pd.Series(
                list(
                    sum(  # to convert lis tof tuples to flat list
                        zip(*([list(range(multipl_factor))] * len(base_list_region))),
                        (),
                    )
                )
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
        start=0, periods=df_testing.shape[0], freq="S", unit="s"
    )
    df_testing["result"] = 1.0
    df = calc_gradient_results(df_testing, "datastream_id")
    pdt.assert_series_equal(
        df.grad, pd.Series(np.zeros_like(df_testing.result), name="grad")
    )


def test_qc_gradient_cacl_one(df_testing):
    df_testing["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
        start=0, periods=df_testing.shape[0], freq="S", unit="s"
    )
    df_testing["result"] = pd.Series(range(df_testing.shape[0]), dtype="float")
    df = calc_gradient_results(df_testing, "datastream_id")
    pdt.assert_series_equal(
        df.grad, pd.Series(np.ones_like(df_testing.result), name="grad")
    )


def test_qc_gradient_cacl_neg_one(df_testing):
    df_testing["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
        start=0, periods=df_testing.shape[0], freq="S", unit="s"
    )
    df_testing["result"] = pd.Series(range(df_testing.shape[0], 0, -1), dtype="float")
    df = calc_gradient_results(df_testing, "datastream_id")
    pdt.assert_series_equal(
        df.grad, pd.Series(np.ones_like(df_testing.result) * -1, name="grad")
    )


def test_qc_gradient_cacl_vardt_pos(df_testing):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"
        ) * list(range(df_slice.shape[0]))
        df_slice["result"] = pd.Series(range(df_slice.shape[0]), dtype="float")
        df = calc_gradient_results(df_slice, "datastream_id")
        pdt.assert_series_equal(
            df.grad,
            pd.Series(
                np.gradient(
                    df_slice.result, [(1 * i**2) for i in range(df_slice.shape[0])]
                ),
                name="grad",
            ),
            check_index=False,
        )


def test_qc_gradient_cacl_vardt_neg(df_testing):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"
        ) * list(range(df_slice.shape[0]))
        df_slice["result"] = pd.Series(range(df_slice.shape[0], 0, -1), dtype="float")
        df = calc_gradient_results(df_slice, "datastream_id")
        pdt.assert_series_equal(
            df.grad,
            pd.Series(
                np.gradient(
                    df_slice.result, [(1 * i**2) for i in range(df_slice.shape[0])]
                ),
                name="grad",
            ),
            check_index=False,
        )


def test_qc_gradient_cacl_vardx_pos(df_testing):
    def grad_cte_dt(fm1, fp1, dh):
        return (fp1 - fm1) / (2.0 * dh)

    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice["phenomenonTime"] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"
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
            df.grad,
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
            start=0, periods=df_slice.shape[0], freq="S", unit="s"
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
            df.grad,
            grad_ref,
            check_index=False,
            check_names=False,
        )
