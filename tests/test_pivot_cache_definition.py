import pytest

from pivot_cache import PivotCacheDefinition


@pytest.fixture
def pivot_cache():
    return PivotCacheDefinition("tests/resources/pivot_table_test.xlsx")


def test_pivot_cache_definition_characteristics(pivot_cache):
    n_columns = 32
    pivot_cache_definition = pivot_cache.parse().pop()
    assert type(pivot_cache_definition) is map
    assert len(list(pivot_cache_definition)) == n_columns
