from databricks.connect import DatabricksSession
import pytest


@pytest.fixture
def spark() -> DatabricksSession:
    """
    Create a spark session. Unit tests don't have access to the spark global
    """
    return DatabricksSession.builder.getOrCreate()


def test_spark(spark):
    """
    Example test that needs to run on the cluster to work
    """
    data = spark.sql("select 4").collect()
    assert data[0][0] == 4