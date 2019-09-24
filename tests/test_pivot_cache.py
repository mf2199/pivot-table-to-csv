from models import PivotCache


def test_pivot_cache_definition_characteristics():
    n_columns = 32
    pivot_cache = PivotCache(pivot_cache_name='pivotCacheDefinition',
                             file_name="tests/resources/pivot_table_test.xlsx")

    pivot_cache_definition = pivot_cache.parse().pop()
    assert type(pivot_cache_definition) is map
    assert len(list(pivot_cache_definition)) == n_columns
