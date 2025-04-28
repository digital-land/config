import pytest
import pandas as pd

from bin.batch_assign_entities import get_field_value_map, get_old_resource_df, get_scope


def test_get_field_value_map():
    data = {
        'entity': [1, 1, 1, 1, 2, 2, 2, 2],
        'field': ['name', 'geometry', 'reference', 'entry-date', 'name', 'geometry' , 'reference', 'entry-date'],
        'value': ['CA1_name', 'POINT()', 'ref1', '2025-01-01', 'CA2_name', 'MULTIPOLYGON()', 'ref2', '2025-01-01']
    }
    df = pd.DataFrame(data)
    result = get_field_value_map(df, 1)
    assert result == {'name': 'CA1_name', 'geometry': 'POINT()'}
    assert 'reference' and 'entry-date' not in result

    result = get_field_value_map(df, 2)
    assert result == {'name': 'CA2_name', 'geometry': 'MULTIPOLYGON()'}
    assert 'reference' and 'entry-date' not in result


def test_get_old_resource_df(mocker):
    
    mock_response = mocker.MagicMock()
    mock_response.text = "resource\nresource_hash\n"

    mock_response_transformed = mocker.MagicMock()
    mock_response_transformed.text = (
        "entity,field,value\n"
        "1,name,test_name\n"
        "1,geometry,POINT()\n"
        "1,reference,ref1"
    )
    mocker.patch("bin.batch_assign_entities.requests.get", side_effect=[mock_response, mock_response_transformed])
    df = get_old_resource_df("test-endpoint", "test-collection", "test-dataset")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["entity", "field", "value"]
    
    expected_fields = ["name", "geometry", "reference"]
    expected_values = ["test_name", "POINT()", "ref1"]

    assert df["field"].tolist() == expected_fields
    assert df["value"].tolist() == expected_values


def test_get_scope():
    scope_dict = {
        "odp": ["conservation-area", "article-4-direction"],
        "mandated": ["brownfield-land", "developer-contributions"],
    }

    assert get_scope("article-4-direction",scope_dict) == "odp"
    assert get_scope("brownfield-land",scope_dict) == "mandated"
    assert get_scope("ancient-woodland",scope_dict) == "single-source"