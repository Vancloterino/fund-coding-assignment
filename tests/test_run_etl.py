from unittest.mock import patch
from run_etl import run_etl


def test_run_etl():
    """Test the run_etl function to ensure all steps are called."""
    with patch("src.setup.Setup.setup_step") as mock_setup, patch(
        "src.transform.Transform.transform_step"
    ) as mock_transform, patch("src.load.Load.load_step") as mock_load:

        run_etl()

        mock_setup.assert_called_once()
        mock_transform.assert_called_once()
        mock_load.assert_called_once()
