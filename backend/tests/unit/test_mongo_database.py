# tests/unit/test_mongo_database.py
import unittest
from unittest.mock import patch, MagicMock
from pymongo import errors
from backend.data.repositories.mongo import MongoDBHandler
from bson import ObjectId

class TestMongoDBHandler(unittest.TestCase):

    def setUp(self):
        """
        Setup the test environment by initializing MongoDBHandler with mock parameters.
        This will also mock MongoDB connection, avoiding real database operations.
        """
        self.db_name = 'test_db'
        self.collection_name = 'test_collection'
        self.mongo_handler = MongoDBHandler(db_name=self.db_name, collection_name=self.collection_name)

        # Sample data for CRUD operations
        self.sample_data = {
            "_id": str(ObjectId()),  # Generate a unique ObjectId for each test
            "name": "Test Document",
            "description": "This is a test document",
            "value": 42,
        }

    @patch('backend.data.repositories.mongo.MongoDBHandler._get_mongo_client')
    def test_mongo_connection(self, mock_get_mongo_client):
        """
        Test if MongoDBHandler successfully connects to the database and collection.
        """
        # Mock MongoDB client, database, and collection
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        # Set the name attribute for the collection
        mock_collection.name = self.collection_name
        
        # Configure the mock client to return the mock database and collection
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        
        # Assign the mock client to the _get_mongo_client return value
        mock_get_mongo_client.return_value = mock_client
        
        # Instantiate the handler
        mongo_handler = MongoDBHandler(db_name=self.db_name, collection_name=self.collection_name)
        
        # Verify that the collection name is set correctly
        self.assertEqual(mongo_handler.collection.name, self.collection_name)


    @patch('backend.data.repositories.mongo.MongoDBHandler.read')
    def test_read_documents(self, mock_read):
        """
        Test reading documents from a MongoDB collection.
        """
        mock_read.return_value = [self.sample_data]  # Mock the return value

        result = self.mongo_handler.read(query={"_id": self.sample_data["_id"]}, collection_name=self.collection_name)
        self.assertEqual(result[0]["_id"], self.sample_data["_id"])

    @patch('backend.data.repositories.mongo.MongoDBHandler.create')
    def test_create_document(self, mock_create):
        """
        Test inserting a document into the collection.
        """
        mock_create.return_value = self.sample_data["_id"]  # Mock the inserted ID

        inserted_id = self.mongo_handler.create(self.sample_data)
        self.assertEqual(inserted_id, self.sample_data["_id"])

    @patch('backend.data.repositories.mongo.MongoDBHandler.update')
    def test_update_document(self, mock_update):
        """
        Test updating a document in the collection.
        """
        mock_update.return_value = 1  # Mock the count of updated documents

        updated_count = self.mongo_handler.update({"_id": self.sample_data["_id"]}, {"value": 100})
        self.assertEqual(updated_count, 1)

    @patch('backend.data.repositories.mongo.MongoDBHandler.delete')
    def test_delete_document(self, mock_delete):
        """
        Test deleting a document from the collection.
        """
        mock_delete.return_value = 1  # Mock the count of deleted documents

        deleted_count = self.mongo_handler.delete({"_id": self.sample_data["_id"]})
        self.assertEqual(deleted_count, 1)

    @patch('backend.data.repositories.mongo.MongoDBHandler.populate_sqlite_from_mongo')
    def test_populate_sqlite_from_mongo(self, mock_populate_sqlite_from_mongo):
        """
        Test the MongoDB to SQLite data migration.
        """
        mock_sqlite_db = MagicMock()
        mock_populate_sqlite_from_mongo.return_value = None  # Mock the migration

        self.mongo_handler.populate_sqlite_from_mongo(
            sqlite_db=mock_sqlite_db,
            collection_name=self.collection_name,
            instrument="EUR_USD",
            granularity="D"
        )
        mock_populate_sqlite_from_mongo.assert_called_once()

    @patch('backend.data.repositories.mongo.MongoDBHandler.close')
    def test_close_connection(self, mock_close):
        """
        Test closing the MongoDB connection.
        """
        self.mongo_handler.close()
        mock_close.assert_called_once() 

    def tearDown(self):
        """
        Teardown the test environment, closing any open MongoDB connections.
        """
        self.mongo_handler.close()

if __name__ == '__main__':
    unittest.main()
                                            