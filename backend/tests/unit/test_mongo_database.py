# tests/unit/test_mongo_handler.py

import unittest
from pymongo import errors
from bson import ObjectId
from data.repositories.mongo import MongoDBHandler

class TestMongoDBHandler(unittest.TestCase):

    def setUp(self):
        """
        Setup the test environment by connecting to the test database and clearing the test collection.
        """
        self.mongo_handler = MongoDBHandler(db_name='sample_airbnb', collection_name='listingsAndReviews')
        self.sample_data = {
            "_id": str(ObjectId()),  # Generate a unique ObjectId for each test
            "listing_url": "https://www.airbnb.com/rooms/10006546",
            "name": "Ribeira Charming Duplex",
            "summary": "Fantastic duplex apartment with three bedrooms, located in the histori…",
            "space": "Privileged views of the Douro River and Ribeira square, our apartment …",
            "description": "Fantastic duplex apartment with three bedrooms, located in the histori…",
            "neighborhood_overview": "In the neighborhood of the river, you can find several restaurants as …",
            "notes": "Lose yourself in the narrow streets and staircases zone, have lunch in…",
            "transit": "Transport: • Metro station and S. Bento railway 5min; • Bus stop a 50 …",
            "access": "We are always available to help guests. The house is fully available t…",
            "interaction": "Cot - 10 € / night Dog - € 7,5 / night",
            "house_rules": "Make the house your home...",
            "property_type": "House",
            "room_type": "Entire home/apt",
            "bed_type": "Real Bed",
            "minimum_nights": "2",
            "maximum_nights": "30",
            "cancellation_policy": "moderate",
            "last_scraped": "2019-02-16T05:00:00.000+00:00",
            "calendar_last_scraped": "2019-02-16T05:00:00.000+00:00",
            "first_review": "2016-01-03T05:00:00.000+00:00",
            "last_review": "2019-01-20T05:00:00.000+00:00",
            "accommodates": 8,
            "bedrooms": 3,
            "beds": 5
        }
        # Insert the sample data
        self.mongo_handler.create(self.sample_data)

    def tearDown(self):
        """
        Teardown the test environment by removing the inserted document.
        """
        self.mongo_handler.delete({"_id": self.sample_data["_id"]})
        self.mongo_handler.close()

    def test_create(self):
        """
        Test creating a new document in the collection.
        """
        # Insert another document with a different _id
        new_data = self.sample_data.copy()
        new_data["_id"] = str(ObjectId())  # New unique ObjectId
        created_id = self.mongo_handler.create(new_data)
        self.assertIsNotNone(created_id)
        self.assertEqual(str(created_id), new_data["_id"])

    def test_read(self):
        """
        Test reading a document from the collection.
        """
        result = self.mongo_handler.read({"_id": self.sample_data["_id"]})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], self.sample_data["name"])

    def test_update(self):
        """
        Test updating a document in the collection.
        """
        updated_count = self.mongo_handler.update({"_id": self.sample_data["_id"]}, {"bedrooms": 4})
        self.assertEqual(updated_count, 1)
        updated_doc = self.mongo_handler.read({"_id": self.sample_data["_id"]})
        self.assertEqual(updated_doc[0]["bedrooms"], 4)

    def test_delete(self):
        """
        Test deleting a document from the collection.
        """
        deleted_count = self.mongo_handler.delete({"_id": self.sample_data["_id"]})
        self.assertEqual(deleted_count, 1)
        result = self.mongo_handler.read({"_id": self.sample_data["_id"]})
        self.assertEqual(len(result), 0)

    def test_drop_database(self):
        """
        Test dropping the entire database.
        """
        # Drop the database
        self.mongo_handler.drop_database('sample_airbnb')

        # Try to access the collection again
        db = self.mongo_handler.client['sample_airbnb']
        collection_names = db.list_collection_names()

        # Assert that the collection no longer exists
        self.assertNotIn('listingsAndReviews', collection_names)
        
if __name__ == '__main__':
    unittest.main()
