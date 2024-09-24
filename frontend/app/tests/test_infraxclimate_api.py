import pytest
from unittest.mock import MagicMock, patch
from infraxclimate_api import infraXclimateAPI, infraXclimateInput

@pytest.fixture
def mock_conn():
    with patch('psycopg2.connect') as mock_connect:
        mock_conn_instance = MagicMock()
        mock_connect.return_value = mock_conn_instance
        yield mock_conn_instance

def test_get_data(mock_conn):
    # Arrange
    input_params = infraXclimateInput(
        category='infrastructure',
        osm_types=['type1', 'type2'],
        # ... other parameters
    )
    api = infraXclimateAPI(conn=mock_conn)
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ({'type': 'FeatureCollection', 'features': []},)
    ]

    # Act
    result = api.get_data(input_params)

    # Assert
    assert result == {'type': 'FeatureCollection', 'features': []}