from src.main import main
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark_session():
    """Fixture to create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_main(spark_session):
    """Test the main function."""
    # Capture the output of main()
    import io
    from contextlib import redirect_stdout

    f = io.StringIO()
    with redirect_stdout(f):
        main()

    # Check if the output contains the expected result
    output = f.getvalue()
    assert "[INFO] DataFrame created successfully" in output
    assert "Charlie" in output  # Filtered row
