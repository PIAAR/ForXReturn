import unittest
from unittest.mock import MagicMock, patch
from api.services.data_population_service import DataPopulationService

class TestDataPopulationService(unittest.TestCase):

    @patch('api.services.data_population_service.MongoDBHandler')
    @patch('api.services.data_population_service.OandaClient')
    def test_populate_historical_data(self, MockOandaClient, MockMongoDBHandler):
        """
        Test fetching and populating historical data for a specific forex pair.
        """
        # Mock OandaClient
        mock_oanda_client_instance = MockOandaClient.return_value
        mock_oanda_client_instance.fetch_historical_data.return_value = [
            {
                "time": "2024-09-03T11:25:00.000000000Z",
                "volume": 100,
                "mid": {"o": 1.2, "h": 1.25, "l": 1.18, "c": 1.22}
            },
            {
                "time": "2024-09-03T11:26:00.000000000Z",
                "volume": 110,
                "mid": {"o": 1.22, "h": 1.26, "l": 1.19, "c": 1.24}
            }
        ]

        # Mock MongoDBHandler
        mock_mongo_handler_instance = MockMongoDBHandler.return_value
        mock_mongo_handler_instance.read.return_value = []  # Return empty, so it proceeds to insert
        mock_mongo_handler_instance.short_bulk_insert = MagicMock()

        # Initialize DataPopulationService
        data_population_service = DataPopulationService()

        # Call populate_historical_data
        data_population_service.mongo_handler.populate_historical_data("EUR_USD", "M1", 500)

        # Assert that bulk_insert was called
        mock_mongo_handler_instance.short_bulk_insert.assert_called_once()

        # Ensure that the correct data is passed to bulk_insert
        args, kwargs = mock_mongo_handler_instance.short_bulk_insert.call_args
        self.assertGreater(len(args[0]), 0)  # Ensure data was passed for insertion


if __name__ == "__main__":
    unittest.main()
