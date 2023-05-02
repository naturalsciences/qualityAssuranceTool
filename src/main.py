import hydra


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    pass


if __name__ == "__main__":
    main()
