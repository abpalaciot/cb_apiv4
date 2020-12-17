from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
import utils


def press_references(event, context):
    """
        To fetch and update the Press References entity.
    """

    print("\n-------Getting Press References entities-------\n")
    COLLECTION_NAME = 'press_reference_entities'
    END_POINT = 'searches/press_references'
    TODAY_DATE = utils.get_today_date()
    YESTURDAY_DATE = utils.get_yesterday_date()

    QUERY = {
        "field_ids": [
            "activity_entities",
            "author",
            "created_at",
            "entity_def_id",
            "identifier",
            "posted_on",
            "publisher",
            "thumbnail_url",
            "title",
            "updated_at",
            "url",
            "uuid"
        ],
        "query": [
            {
                "type": "predicate",
                "field_id": "updated_at",
                "operator_id": "gte",
                "values": [str(YESTURDAY_DATE)]
            },
        ],
        "order": [
            {
                "field_id": "updated_at",
                "sort": "asc",
                "nulls": "last"
            }
        ],
        "limit": 1000,
    }

    print("\nFetching the entities please wait. This will take sometime.")
    total_count, entities = utils.fetch_data(QUERY, END_POINT)
    if total_count is None:
        return "Error in parsing the API response. Please check the logs."

    print("total count: ", total_count)

    # get the press_references collection
    col = utils.get_mongodb_collection(COLLECTION_NAME)

    fetch_records_count = 0
    
    # storing into the database and pagination
    while fetch_records_count < total_count:
        if fetch_records_count != 0:
            _, entities = utils.fetch_data(QUERY, END_POINT)

        if not entities:
            print("no entities left i.e., entities = %s. moving on." % len(entities))
            break

        for e in entities:
            if e:
                e['insert_date'] = TODAY_DATE
            else:
                print("Entity is empty: ", e)

        col.insert_many(entities)
        fetch_records_count += len(entities)
        # print("inserted records: ")
        # pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        print("------------------------")

        # get the last record
        after_id = entities[-1].get('uuid', None)
        if after_id:
            # print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()

    msg = {'entity': 'press_references',
           'total_record_updated': fetch_records_count}
    print("\nTask Done: ", msg)

