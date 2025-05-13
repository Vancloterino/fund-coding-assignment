from src.setup import Setup
from src.load import Load
from src.transform import Transform


def run_etl():
    Setup.setup_step()
    Transform.transform_step()
    Load.load_step()


if __name__ == "__main__":
    run_etl()
