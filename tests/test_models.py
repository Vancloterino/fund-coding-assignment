import tempfile

import pytest
from pydantic import ValidationError

from src.models.models import Config


def test_config_validation():
    """
    Test the Config Pydantic model for validation.
    """
    # Test with valid directories
    with tempfile.TemporaryDirectory() as input_dir, tempfile.TemporaryDirectory() as output_dir:
        try:
            Config(input_directory=input_dir, output_directory=output_dir)
        except ValidationError:
            pytest.fail("Config validation failed for valid directories.")

    # Test with invalid input_directory
    with tempfile.TemporaryDirectory() as output_dir:
        with pytest.raises(ValidationError):
            Config(
                input_directory="/non/existent/input_dir", output_directory=output_dir
            )

    # Test with invalid output_directory
    with tempfile.TemporaryDirectory() as input_dir:
        with pytest.raises(ValidationError):
            Config(
                input_directory=input_dir, output_directory="/non/existent/output_dir"
            )
