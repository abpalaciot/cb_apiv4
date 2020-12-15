from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
from flask import jsonify
import utils


def press_references(request):
    """
        To fetch and update the Press References entity.
    """

    print("\n-------Getting Press References entities-------\n")
    COLLECTION_NAME = 'press_references'
    END_POINT = 'searches/press_references'

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

    total_count, entities = utils.fetch_data(QUERY, END_POINT)
    if total_count is None:
        return "Error in parsing the API response. Please check the logs."

    print("total count: ", total_count)

    # get the press_references collection
    col = utils.get_mongodb_collection(COLLECTION_NAME)

    fetch_records_count = 0
    operations = []

    # storing into the database and pagination
    while fetch_records_count < total_count:
        if fetch_records_count != 0:
            _, entities = utils.fetch_data(QUERY, END_POINT)

        for e in entities:
            # filter = {'uuid': e['uuid']}
            if e:
                operations.append(
                    ReplaceOne({'uuid': e['uuid']}, e, upsert=True)
                )
            else:
                print("Entity is empty: ", e)
            # print("The filter is: ", filter)
            # uuid_list.append(e['properties']['updated_at'])

        try:
            if len(operations) > 0:
                bulk_results = col.bulk_write(operations, ordered=False)
                pprint(bulk_results.bulk_api_result)
                fetch_records_count += len(entities)
            else:
                print("No operation left (len = %s). Moving on." %
                      len(operations))
                print("the entities list: ", entities)
        except BulkWriteError as bwe:
            pprint(bwe.details)

        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        # get the last record (for pagination)
        if not entities:
            break

        after_id = entities[-1].get('uuid', None)
        if after_id:
            print("print next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        print("------------------------")
        operations.clear()
        entities.clear()

    msg = {'entity': 'press_references',
           'total_record_updated': fetch_records_count}
    return jsonify(msg)
