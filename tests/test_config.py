import pytest
from hydra import compose, initialize
from omegaconf import DictConfig
from pandasta.sta_requests import set_sta_url

from services.qualityassurancetool.config import (QCconf, filter_cfg_to_query,
                                                  get_date_from_string)


@pytest.fixture(scope="session")
def cfg() -> DictConfig:
    with initialize(config_path="./conf", version_base="1.2"):
        conf = compose("conf_base.yaml")
    set_sta_url(conf.data_api.base_url)

    return conf


class TestConfig:
    def test_hydra_is_loaded(self):
        print(cfg)
        assert cfg

    def test_filter_cfg_to_query(self, cfg: QCconf):
        out = filter_cfg_to_query(cfg.data_api.filter)
        assert (
            out == "phenomenonTime gt 1002-01-01T00:00:00.000000Z and "
            "phenomenonTime lt 3003-01-01T00:00:00.000000Z"
        )

    def test_get_date_from_string(self):
        # date_o = get_date_from_string("2023-04-01 12:15", "%Y-%m-%d %H:%M", "%Y%m%d")
        date_o = get_date_from_string("2023-04-01 12:15")
        assert date_o == "20230401"
