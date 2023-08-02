from itertools import product
from operator import add
from typing import Sequence

import geopandas as gpd
import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from models.enums import Df, QualityFlags
from services.qc import (
    calc_gradient_results,
    qc_dependent_quantity_base,
    qc_dependent_quantity_secondary,
    qc_region,
)
from services.qc import get_bool_spacial_outlier_compared_to_median
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


base_list_region: list = [
    "NORTH SEA",
    "MAINLAND EUROPE",
    "MAINLAND random",
    None,
    np.nan,
]


@pytest.fixture
def df_testing() -> gpd.GeoDataFrame:
    results_factor: float = 2.345

    base_list_phenomenonTime: list[pd.Timestamp] = list(
        pd.Timestamp("now")
        + pd.timedelta_range(
            start=0, periods=len(base_list_region), freq="S", unit="s"  # type: ignore
        )
    )
    base_results: list[float] = [
        fi * results_factor for fi in range(len(base_list_region))
    ]
    qc_ref_base: list[QualityFlags | float] = [
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

    datastream_id_series: "pd.Series[int]" = pd.Series(
        list(
            sum(  # to convert list of tuples to flat list
                zip(*([list(range(MULTIPL_FACTOR))] * len(base_list_region))), ()
            )
        ),
        dtype=int,
    )
    df_out = gpd.GeoDataFrame(
        {
            Df.IOT_ID: pd.Series(range(len(qc_ref_base) * MULTIPL_FACTOR), dtype=int),
            Df.REGION: pd.Series(base_list_region * MULTIPL_FACTOR, dtype="string"),
            Df.QC_FLAG
            + "_ref": pd.Series(qc_ref_base * MULTIPL_FACTOR),
            Df.DATASTREAM_ID: pd.Series(
                list(
                    sum(  # to convert list of tuples to flat list
                        zip(*([list(range(MULTIPL_FACTOR))] * len(base_list_region))),
                        (),
                    )
                ),
                dtype=int,
            ),
            Df.TIME: pd.Series(
                base_list_phenomenonTime * MULTIPL_FACTOR
            ),
            Df.RESULT: pd.Series(
                map(
                    add,
                    base_results * MULTIPL_FACTOR,
                    [i * 10 for i in datastream_id_series.to_list()],
                ),
                dtype=float,
            ),
            Df.OBSERVATION_TYPE: pd.Series(
                base_observation_type * MULTIPL_FACTOR, dtype="category"
            ),
        }
    )

    return df_out


MULTIPL_FACTOR: int = 2
LENGTH: int = len(base_list_region) * MULTIPL_FACTOR


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
    # pandas does strange things with type hints
    df_out = qc_region(df_testing)
    pdt.assert_series_equal(
        df_out.loc[:,Df.QC_FLAG],
        df_out.loc[:, Df.QC_FLAG + "_ref"],
        check_names=False,
    )


@pytest.mark.parametrize(
    "idx,dx,columns",
    [
        ([1, 4], 1, [Df.LONG]),
        ([3, 4], 1, [Df.LAT]),
        ([3, 4], -0.1, [Df.LONG]),
        ([3, 4], -0.1, [Df.LAT, Df.LONG]),
        ([3, 6], -1, [Df.LAT]),
    ],
)
def test_location_outlier(df_testing, idx, dx, columns):
    df_testing[Df.LONG] = df_testing.index * 0.001 + 50.0
    df_testing[Df.LAT] = df_testing.index * 0.001 + 50.0

    for idx_i, col_i in product(idx, columns):
        df_testing.iloc[idx_i, df_testing.columns.get_loc(col_i)] -= dx

    df_testing["geometry"] = gpd.points_from_xy(df_testing[Df.LONG], df_testing[Df.LAT])
    df_testing = df_testing.set_crs("EPSG:4326")

    res = get_bool_spacial_outlier_compared_to_median(df_testing, max_dx_dt=1e3)
    assert all(res[idx])


@pytest.mark.parametrize(
    "result,ref",
    [
        (np.zeros(LENGTH), np.zeros(LENGTH)),
        (range(0, LENGTH, 1), np.ones(LENGTH)),
        (range(LENGTH, 0, -1), np.ones(LENGTH) * -1),
    ],
)
def test_qc_gradient_calc_basic(df_testing, result, ref):
    df_testing[Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
        start=0, periods=df_testing.shape[0], freq="S", unit="s"  # type: ignore
    )
    df_testing[Df.RESULT] = pd.Series(result, dtype="float")
    df = calc_gradient_results(df_testing, Df.DATASTREAM_ID)
    pdt.assert_series_equal(df[Df.GRADIENT], pd.Series(ref, name=Df.GRADIENT))


def test_qc_gradient_cacl_vardt_pos(df_testing):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice.loc[:, Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        ) * list(range(df_slice.shape[0]))
        df_slice.loc[:, Df.RESULT] = pd.Series(range(df_slice.shape[0]), dtype="float")
        df = calc_gradient_results(df_slice, Df.DATASTREAM_ID)
        pdt.assert_series_equal(
            df.gradient,
            pd.Series(
                np.gradient(
                    df_slice.result, [(1 * i**2) for i in range(df_slice.shape[0])]
                ),
                name=Df.GRADIENT,
            ),
            check_index=False,
        )


def test_qc_gradient_cacl_vardt_neg(df_testing):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice.loc[:, Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        ) * list(range(df_slice.shape[0]))
        df_slice.loc[:, Df.RESULT] = pd.Series(
            range(df_slice.shape[0], 0, -1), dtype="float"
        )
        df = calc_gradient_results(df_slice, Df.DATASTREAM_ID)
        pdt.assert_series_equal(
            df[Df.GRADIENT],
            pd.Series(
                np.gradient(
                    df_slice.result, [(1 * i**2) for i in range(df_slice.shape[0])]
                ),
                name=Df.GRADIENT,
            ),
            check_index=False,
        )


def test_qc_gradient_cacl_vardx_pos(df_testing):
    def grad_cte_dt(fm1, fp1, dh):
        return (fp1 - fm1) / (2.0 * dh)

    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice.loc[:, Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        )

        df_slice.loc[:, Df.RESULT] = pd.Series(
            np.array(list(range(df_slice.shape[0]))) * range(df_slice.shape[0]),
            dtype="float",
        )
        df = calc_gradient_results(df_slice, Df.DATASTREAM_ID)

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
        df_slice.loc[:, Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="S", unit="s"  # type: ignore
        )

        df_slice.loc[:, Df.RESULT] = pd.Series(
            np.array(list(range(df_slice.shape[0], 0, -1))) * range(df_slice.shape[0]),
            dtype="float",
        )
        df = calc_gradient_results(df_slice, Df.DATASTREAM_ID)

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
            Df.RESULT: list(range(4)),
            "flag": [str(i) for i in range(4)],
        }
    )
    df_p = df.pivot(index=["time"], columns=["type"], values=[Df.RESULT, "flag"])
    df_p_undone = (
        df_p.stack()
        .reset_index()
        .sort_values("type")
        .reset_index(drop=True)
        .sort_index(axis=1)
    )
    df_p_undone.result = df_p_undone.result.astype(int)
    pdt.assert_frame_equal(df_p_undone.sort_index(axis=1), df.sort_index(axis=1))


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
def test_qc_dependent_quantities(df_testing, n):
    # setup ref count
    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 2,
        QualityFlags.BAD: 2,
    }

    # setup df
    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == 0].index[n]
    df_testing.loc[idx_, Df.QC_FLAG] = QualityFlags.BAD

    # perform qc check
    df_testing = qc_dependent_quantity_base(df_testing, independent=0, dependent=1)
    # assert
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("bad_value", (100.0,))
@pytest.mark.parametrize("n", (0, 2, 4))
def test_qc_dependent_quantities_secondary_fct(df_testing, bad_value, n):
    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 1,
        QualityFlags.BAD: 1,
    }
    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing[df_testing[Df.DATASTREAM_ID] == 0].index[n]
    df_testing.loc[idx_, Df.RESULT] = bad_value

    df_testing = qc_dependent_quantity_secondary(df_testing, 0, 1, range_=(0.0, 10.0))
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref
    assert (
        df_testing.loc[
            idx_ + int(df_testing.shape[0] / len(df_testing.datastream_id.unique())),
            Df.QC_FLAG,
        ]
        == QualityFlags.BAD
    )
