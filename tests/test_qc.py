from copy import deepcopy
from itertools import cycle, islice, product
from operator import add
from typing import Sequence

import geopandas as gpd
import geopy.distance as gp_distance
import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
from geopy import Point as gp_point

from models.enums import Df, QualityFlags
from services.pandasta.df import df_type_conversions
from services.qualityassurancetool.qc import (CAT_TYPE, calc_gradient_results,
                         get_bool_exceed_max_acceleration,
                         get_bool_exceed_max_velocity, get_bool_land_region,
                         get_bool_null_region, get_bool_out_of_range,
                         get_bool_spacial_outlier_compared_to_median,
                         get_qc_flag_from_bool, qc_dependent_quantity_base,
                         qc_dependent_quantity_secondary)
from services.searegion.queryregion import build_points_query


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
            start=0, periods=len(base_list_region), freq="s", unit="s"  # type: ignore
        )
    )
    base_results: list[float] = [
        fi * results_factor for fi in range(len(base_list_region))
    ]
    qc_ref_base: list[QualityFlags | float] = [
        QualityFlags.NO_QUALITY_CONTROL,
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
            + "_ref": pd.Series(qc_ref_base * MULTIPL_FACTOR, dtype=CAT_TYPE),
            Df.QC_FLAG: pd.Series(
                QualityFlags.NO_QUALITY_CONTROL,  # type: ignore
                index=datastream_id_series.index,
                dtype=CAT_TYPE,
            ),  # type: ignore
            Df.DATASTREAM_ID: pd.Series(
                list(
                    sum(  # to convert list of tuples to flat list
                        zip(*([list(range(MULTIPL_FACTOR))] * len(base_list_region))),
                        (),
                    )
                ),
                dtype=int,
            ),
            Df.TIME: pd.Series(base_list_phenomenonTime * MULTIPL_FACTOR),
            Df.RESULT: pd.Series(
                map(
                    add,
                    base_results * MULTIPL_FACTOR,
                    [i * 10 for i in datastream_id_series.to_list()],
                ),  # type: ignore
                dtype=float,
            ),  # type: ignore
            Df.OBSERVATION_TYPE: pd.Series(
                base_observation_type * MULTIPL_FACTOR, dtype="category"
            ),
        }
    )
    return df_out


@pytest.fixture
def df_velocity_acceleration() -> gpd.GeoDataFrame:
    df_t = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
    df_t[Df.TIME] = pd.to_timedelta(df_t["Time (s)"], "s") + pd.Timestamp("now")

    p0 = gp_point(longitude=3.1840709669760137, latitude=51.37115902107277)
    for index, row_i in df_t.iterrows():
        di = gp_distance.distance(meters=row_i["Distance (m)"])
        pi = di.destination(point=p0, bearing=row_i["Heading (degrees)"])

        df_t.loc[index, [Df.LONG, Df.LAT]] = pi.longitude, pi.latitude  # type: ignore
        p0 = pi

    df_t = df_t.drop(columns=["Time (s)", "Distance (m)", "Heading (degrees)"])
    df_t = gpd.GeoDataFrame(df_t, geometry=gpd.points_from_xy(df_t[Df.LONG], df_t[Df.LAT], crs="EPSG:4326"))  # type: ignore
    return df_t


MULTIPL_FACTOR: int = 2
LENGTH: int = len(base_list_region) * MULTIPL_FACTOR
LENGTH_SLICE: int = len(base_list_region)


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
    # df_out = qc_region(df_testing)
    df_out = deepcopy(df_testing)
    bool_nan = get_bool_null_region(df_out)
    df_out[Df.QC_FLAG] = (
        df_out[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_nan,
                flag_on_true=QualityFlags.PROBABLY_BAD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )

    bool_mainland = get_bool_land_region(df_out)
    df_out[Df.QC_FLAG] = (
        df_out[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_mainland,
                flag_on_true=QualityFlags.BAD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )

    pdt.assert_series_equal(
        df_out.loc[:, Df.QC_FLAG],
        df_out.loc[:, Df.QC_FLAG + "_ref"],
        check_names=False,
    )


def test_qc_region_to_flag_set_on_false(df_testing):
    # pandas does strange things with type hints
    # df_out = qc_region(df_testing)
    df_out = deepcopy(df_testing)
    bool_nan = get_bool_null_region(df_out)
    df_out[Df.QC_FLAG] = (
        df_out[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_nan,
                flag_on_true=QualityFlags.PROBABLY_BAD,
                flag_on_false=QualityFlags.GOOD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )

    bool_mainland = get_bool_land_region(df_out)
    df_out[Df.QC_FLAG] = (
        df_out[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_mainland,
                flag_on_true=QualityFlags.BAD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )

    df_out.loc[
        df_out[Df.QC_FLAG + "_ref"] == QualityFlags.NO_QUALITY_CONTROL,
        Df.QC_FLAG + "_ref",
    ] = QualityFlags.GOOD

    pdt.assert_series_equal(
        df_out.loc[:, Df.QC_FLAG],
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
    df_testing[Df.LAT] = df_testing.index * 0.001 + 20.0

    for idx_i, col_i in product(idx, columns):
        df_testing.iloc[idx_i, df_testing.columns.get_loc(col_i)] -= dx

    # df_testing.set_geometry("geometry")
    df_testing = df_testing.set_geometry(
        gpd.points_from_xy(df_testing[Df.LONG], df_testing[Df.LAT], crs="EPSG:4326")
    )

    res = get_bool_spacial_outlier_compared_to_median(
        df_testing, max_dx_dt=300.0, time_window="5min"
    )
    mask = np.ma.masked_array(res, mask=res)
    assert all(res[idx]) and ~mask.any() and (sum(res) == len(idx))


@pytest.mark.parametrize(
    "idx,column",
    [
        ([1], Df.LONG),
        ([1, 4], Df.LONG),
        ([1, 4], Df.LAT),
        ([4], Df.LAT),
        ([3, 4], Df.LAT),
        ([3, 4], Df.LONG),
        ([3], Df.LONG),
        ([3, 6], Df.LONG),
        ([6], Df.LONG),
    ],
)
def test_location_outlier_eq(df_testing, idx, column):
    df_testing[Df.LONG] = df_testing.index * 0.001 + 50.0
    df_testing[Df.LAT] = df_testing.index * 0.001 + 20.0

    col1 = column
    set_column_tmp = set([Df.LONG, Df.LAT])
    set_column_tmp.remove(column)
    col2 = set_column_tmp.pop()
    for idx_i in idx:
        df_testing.iloc[idx_i, df_testing.columns.get_loc(col1)] = df_testing.iloc[
            idx_i, df_testing.columns.get_loc(col2)
        ]

    # df_testing.set_geometry("geometry")
    df_testing = df_testing.set_geometry(
        gpd.points_from_xy(df_testing[Df.LONG], df_testing[Df.LAT], crs="EPSG:4326")
    )

    res = get_bool_spacial_outlier_compared_to_median(
        df_testing, max_dx_dt=300.0, time_window="5min"
    )
    mask = np.ma.masked_array(res, mask=res)
    assert all(res[idx]) and ~mask.any() and (sum(res) == len(idx))


@pytest.mark.parametrize(
    "idx,columns",
    [
        ([1, 4], [Df.LONG]),
        ([1, 4], [Df.LAT]),
        ([1, 4], [Df.LAT, Df.LONG]),
        ([3, 4], [Df.LAT]),
        ([3, 4], [Df.LONG]),
        ([3, 6], [Df.LONG]),
        ([3, 6], [Df.LONG, Df.LAT]),
    ],
)
def test_location_outlier_zero(df_testing, idx, columns):
    df_testing[Df.LONG] = df_testing.index * 0.001 + 50.0
    df_testing[Df.LAT] = df_testing.index * 0.001 + 20.0

    for idx_i, col_i in product(idx, columns):
        df_testing.iloc[idx_i, df_testing.columns.get_loc(col_i)] = 0

    # df_testing.set_geometry("geometry")
    df_testing = df_testing.set_geometry(
        gpd.points_from_xy(df_testing[Df.LONG], df_testing[Df.LAT], crs="EPSG:4326")
    )

    res = get_bool_spacial_outlier_compared_to_median(
        df_testing, max_dx_dt=300.0, time_window="5min"
    )
    mask = np.ma.masked_array(res, mask=res)
    assert all(res[idx]) and ~mask.any() and (sum(res) == len(idx))


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
def test_location_outlier_long_eq_lat(df_testing, idx, dx, columns):
    df_testing[Df.LONG] = df_testing.index * 0.001 + 50.0
    df_testing[Df.LAT] = df_testing.index * 0.001 + 20.0

    for idx_i, col_i in product(idx, columns):
        df_testing.iloc[idx_i, df_testing.columns.get_loc(col_i)] -= dx
        other_column = [Df.LAT, Df.LONG][col_i == Df.LAT]
        changed_to_value = df_testing.iloc[idx_i, df_testing.columns.get_loc(col_i)]
        df_testing.iloc[idx_i, df_testing.columns.get_loc(other_column)] = (
            changed_to_value
        )

    df_testing = df_testing.set_geometry(
        gpd.points_from_xy(df_testing[Df.LONG], df_testing[Df.LAT], crs="EPSG:4326")
    )

    res = get_bool_spacial_outlier_compared_to_median(
        df_testing, max_dx_dt=300, time_window="5min"
    )
    mask = np.ma.masked_array(res, mask=res)
    assert all(res[idx]) and ~mask.any() and (sum(res) == len(idx))


def test_exceed_max_velocity(df_velocity_acceleration):
    res = get_bool_exceed_max_velocity(df_velocity_acceleration, max_velocity=90)
    assert all(~res)


def test_exceed_max_velocity_2(df_velocity_acceleration):
    res = get_bool_exceed_max_velocity(df_velocity_acceleration, max_velocity=0)
    assert all(res)


def test_exceed_max_velocity_3(df_velocity_acceleration):
    bool_ref = get_bool_exceed_max_velocity(df_velocity_acceleration, max_velocity=1e12)
    bool_ref.loc[3] = True  # type: ignore
    df_velocity_acceleration.loc[4, Df.TIME] = df_velocity_acceleration.loc[3, Df.TIME] + pd.Timedelta(nanoseconds=1)  # type: ignore
    res = get_bool_exceed_max_velocity(df_velocity_acceleration, max_velocity=90)

    pdt.assert_series_equal(res, bool_ref, check_names=False)


@pytest.mark.parametrize(
    "idx0",
    [
        1,
        2,
        3,
        4,
        5,
        6,
    ],
)
def test_exceed_max_acceleration(df_velocity_acceleration, idx0):
    bool_ref = get_bool_exceed_max_acceleration(
        df_velocity_acceleration, max_acceleration=1e12
    )
    bool_ref.loc[[idx0 - 1, idx0]] = True
    df_velocity_acceleration.loc[idx0 + 1, Df.TIME] = df_velocity_acceleration.loc[idx0, Df.TIME] + pd.Timedelta(nanoseconds=1)  # type: ignore
    res = get_bool_exceed_max_acceleration(
        df_velocity_acceleration, max_acceleration=25
    )

    pdt.assert_series_equal(res, bool_ref, check_names=False)


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
        start=0, periods=df_testing.shape[0], freq="s", unit="s"  # type: ignore
    )
    df_testing[Df.RESULT] = pd.Series(result, dtype="float")
    df = calc_gradient_results(df_testing, Df.DATASTREAM_ID)
    pdt.assert_series_equal(df[Df.GRADIENT], pd.Series(ref, name=Df.GRADIENT))


@pytest.mark.parametrize("result", [(range(0, LENGTH, 1)), (range(LENGTH, 0, -1))])
def test_qc_gradient_cacl_vardt(df_testing, result):
    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice.loc[:, Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="s", unit="s"  # type: ignore
        ) * list(range(df_slice.shape[0]))
        df_slice.loc[:, Df.RESULT] = pd.Series(result, dtype="float")
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


@pytest.mark.parametrize(
    "result", [(range(0, LENGTH_SLICE, 1)), (range(LENGTH_SLICE, 0, -1))]
)
def test_qc_gradient_cacl_vardx(df_testing, result):
    def grad_cte_dt(fm1, fp1, dh):
        return (fp1 - fm1) / (2.0 * dh)

    for ds_i in df_testing.datastream_id.unique():
        df_slice = df_testing.loc[df_testing.datastream_id == ds_i]
        df_slice.loc[:, Df.TIME] = pd.Timestamp("now") + pd.timedelta_range(
            start=0, periods=df_slice.shape[0], freq="s", unit="s"  # type: ignore
        )

        df_slice.loc[:, Df.RESULT] = pd.Series(
            np.array(result, "int") * range(df_slice.shape[0]),
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
        df_p.stack(future_stack=True)  # type: ignore
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
    qc_flag_update = qc_dependent_quantity_base(
        df_testing, independent=0, dependent=1, dt_tolerance="0.5s"
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_flag_update})
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
@pytest.mark.parametrize("m", tuple(range(1, len(base_list_region))))
def test_qc_dependent_quantities_missing_dependent(df_testing, n, m):
    # setup ref count
    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 3,
        QualityFlags.BAD: 2,
    }

    # setup df
    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == 0].index[n]
    df_testing.loc[idx_, Df.QC_FLAG] = QualityFlags.BAD
    pool = cycle(range(len(base_list_region)))
    l = next(pool)
    for i in range(n + m):
        l = next(pool)
    idx_remove = df_testing.loc[df_testing[Df.DATASTREAM_ID] == 1].index[l]

    df_testing = df_testing.drop(index=idx_remove).reset_index()

    # perform qc check
    qc_flag_update = qc_dependent_quantity_base(
        df_testing, independent=0, dependent=1, dt_tolerance="0.5s"
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_flag_update})
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


def test_qc_range(df_testing):
    df_testing.loc[
        df_testing[Df.DATASTREAM_ID] == 0, ["qc_range_min", "qc_range_max"]
    ] = [2.0, 9.2]
    bool_range = get_bool_out_of_range(df=df_testing, qc_on=Df.RESULT, qc_type="range")
    bool_ref = pd.Series(
        [True, False, False, False, True],
        index=df_testing.loc[df_testing[Df.DATASTREAM_ID] == 0].index,
        dtype=bool,
    )
    pdt.assert_series_equal(bool_ref, bool_range, check_names=False)


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
def test_qc_dependent_quantities_mismatch(df_testing, n):
    # setup ref count
    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 1,
        QualityFlags.BAD: 1,
    }

    # setup df
    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == 0].index[n]
    df_testing.loc[idx_, Df.TIME] += pd.Timedelta("1d")

    # perform qc check
    qc_update = qc_dependent_quantity_base(
        df_testing, independent=0, dependent=1, dt_tolerance="0.5s"
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_update})
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
def test_qc_dependent_quantities_base_3streams(df_testing, n):
    df_additional = df_testing.loc[df_testing[Df.DATASTREAM_ID] == 1]
    df_additional.loc[:, Df.DATASTREAM_ID] = 10
    df_additional.loc[:, Df.IOT_ID] = (
        df_additional[Df.IOT_ID] + df_additional[Df.DATASTREAM_ID]
    )
    df_testing = pd.concat([df_testing, df_additional], ignore_index=True)
    df_testing = df_type_conversions(df_testing)

    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == 0].index[n]
    df_testing.loc[idx_, Df.QC_FLAG] = QualityFlags.BAD

    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 2,
        QualityFlags.BAD: 2,
    }
    qc_update = qc_dependent_quantity_base(
        df_testing, independent=0, dependent=1, dt_tolerance="0.5s"
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_update})
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
@pytest.mark.parametrize("n_rel_del", tuple(range(1, len(base_list_region))))
@pytest.mark.parametrize(
    "independent_id, dependent_id",
    [(0, 1), (1, 0)],
)
def test_qc_dependent_quantities_base_3streams_missing(
    df_testing, n, n_rel_del, independent_id, dependent_id
):
    df_additional = df_testing.loc[df_testing[Df.DATASTREAM_ID] == dependent_id]
    df_additional.loc[:, Df.DATASTREAM_ID] = 10
    df_additional.loc[:, Df.IOT_ID] = (
        df_additional[Df.IOT_ID] + df_additional[Df.DATASTREAM_ID]
    )
    df_testing = pd.concat([df_testing, df_additional], ignore_index=True)
    df_testing = df_type_conversions(df_testing)

    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == independent_id].index[n]
    idx_delete = next(
        islice(
            cycle(
                df_testing.loc[
                    df_testing[Df.DATASTREAM_ID] == independent_id
                ].index.values
            ),
            n + n_rel_del,
            None,
        )
    )
    df_testing.loc[idx_, Df.QC_FLAG] = QualityFlags.BAD
    df_testing = df_testing.drop(idx_delete).reset_index()

    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 3,
        QualityFlags.BAD: 3,
    }
    qc_update = qc_dependent_quantity_base(
        df_testing,
        independent=independent_id,
        dependent=dependent_id,
        dt_tolerance="0.5s",
        flag_when_missing=QualityFlags.BAD,
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_update})
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
@pytest.mark.parametrize("n_rel_del", tuple(range(1, len(base_list_region))))
@pytest.mark.parametrize(
    "independent_id, dependent_id",
    [(0, 1), (1, 0)],
)
def test_qc_dependent_quantities_base_3streams_missing_dependent(
    df_testing, n, n_rel_del, independent_id, dependent_id
):
    df_additional = df_testing.loc[df_testing[Df.DATASTREAM_ID] == dependent_id]
    df_additional.loc[:, Df.DATASTREAM_ID] = 10
    df_additional.loc[:, Df.IOT_ID] = (
        df_additional[Df.IOT_ID] + df_additional[Df.DATASTREAM_ID]
    )
    df_testing = pd.concat([df_testing, df_additional], ignore_index=True)
    df_testing = df_type_conversions(df_testing)

    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == independent_id].index[n]
    idx_delete = next(
        islice(
            cycle(
                df_testing.loc[
                    df_testing[Df.DATASTREAM_ID] == dependent_id
                ].index.values
            ),
            n + n_rel_del,
            None,
        )
    )
    df_testing.loc[idx_, Df.QC_FLAG] = QualityFlags.BAD
    df_testing = df_testing.drop(idx_delete).reset_index()

    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 2,
        QualityFlags.BAD: 2,
    }
    qc_update = qc_dependent_quantity_base(
        df_testing,
        independent=independent_id,
        dependent=dependent_id,
        dt_tolerance="0.5s",
        flag_when_missing=QualityFlags.BAD,
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_update})
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("n", tuple(range(len(base_list_region))))
@pytest.mark.parametrize("n_rel_del", tuple(range(1, len(base_list_region))))
@pytest.mark.parametrize(
    "independent_id, dependent_id",
    [(0, 1), (1, 0)],
)
def test_qc_dependent_quantities_base_3streams_missing_noflag(
    df_testing, n, n_rel_del, independent_id, dependent_id
):
    df_additional = df_testing.loc[df_testing[Df.DATASTREAM_ID] == dependent_id]
    df_additional.loc[:, Df.DATASTREAM_ID] = 10
    df_additional.loc[:, Df.IOT_ID] = (
        df_additional[Df.IOT_ID] + df_additional[Df.DATASTREAM_ID]
    )
    df_testing = pd.concat([df_testing, df_additional], ignore_index=True)
    df_testing = df_type_conversions(df_testing)

    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing.loc[df_testing[Df.DATASTREAM_ID] == independent_id].index[n]
    idx_delete = next(
        islice(
            cycle(
                df_testing.loc[
                    df_testing[Df.DATASTREAM_ID] == independent_id
                ].index.values
            ),
            n + n_rel_del,
            None,
        )
    )
    df_testing.loc[idx_, Df.QC_FLAG] = QualityFlags.BAD
    df_testing = df_testing.drop(idx_delete).reset_index()

    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 2,
        QualityFlags.BAD: 2,
    }
    qc_update = qc_dependent_quantity_base(
        df_testing,
        independent=independent_id,
        dependent=dependent_id,
        dt_tolerance="0.5s",
        flag_when_missing=None,
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update(qc_update)
    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref


@pytest.mark.parametrize("bad_value", (100.0,))
@pytest.mark.parametrize("n", (0, 2, 4))
@pytest.mark.parametrize(
    "independent_id, dependent_id",
    [
        (0, 1),
    ],
)
def test_qc_dependent_quantities_secondary_fct(
    df_testing, bad_value, n, independent_id, dependent_id
):
    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 1,
        QualityFlags.BAD: 1,
    }
    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing[df_testing[Df.DATASTREAM_ID] == independent_id].index[n]
    df_testing.loc[idx_, Df.RESULT] = bad_value

    qc_update = qc_dependent_quantity_secondary(
        df_testing,
        independent=independent_id,
        dependent=dependent_id,
        range_=(0.0, 10.0),
        dt_tolerance="0.5s",
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update(qc_update)

    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref
    assert (
        df_testing.loc[
            idx_ + int(df_testing.shape[0] / len(df_testing.datastream_id.unique())),
            Df.QC_FLAG,
        ]
        == QualityFlags.BAD
    )


@pytest.mark.parametrize("bad_value", (100.0,))
@pytest.mark.parametrize("n", (0, 2, 4))
@pytest.mark.parametrize("m", (1, 3))
@pytest.mark.parametrize(
    "independent_id, dependent_id",
    [
        (0, 1),
    ],
)
def test_qc_dependent_quantities_secondary_fct_missing_dependent(
    df_testing, bad_value, n, m, independent_id, dependent_id
):
    qc_flag_count_ref = {
        QualityFlags.GOOD: df_testing.shape[0] - 2,
        QualityFlags.BAD: 1,
    }
    df_testing[Df.QC_FLAG] = QualityFlags.GOOD

    idx_ = df_testing[df_testing[Df.DATASTREAM_ID] == independent_id].index[n]
    df_testing.loc[idx_, Df.RESULT] = bad_value

    pool = cycle(range(len(base_list_region)))
    l = next(pool)
    for i in range(n + m):
        l = next(pool)
    idx_remove = df_testing.loc[df_testing[Df.DATASTREAM_ID] == independent_id].index[l]

    df_testing = df_testing.drop(index=idx_remove).reset_index()

    qc_update = qc_dependent_quantity_secondary(
        df_testing,
        independent=independent_id,
        dependent=dependent_id,
        range_=(0.0, 10.0),
        dt_tolerance="0.5s",
    )
    df_testing = df_testing.set_index(Df.IOT_ID)
    df_testing.update({Df.QC_FLAG: qc_update})

    assert df_testing[Df.QC_FLAG].value_counts().to_dict() == qc_flag_count_ref
    assert (
        df_testing.loc[
            idx_
            + int(
                (df_testing.shape[0] + 1) / len(df_testing[Df.DATASTREAM_ID].unique())
            ),  # TODO: refactoring!
            Df.QC_FLAG,
        ]
        == QualityFlags.BAD
    )
