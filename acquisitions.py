from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
from flask import jsonify
import utils

def acquisitions(request):
    """
        To fetch and update the Acquisitions entity.
    """

    print("\n-------Getting Acquisitions entities-------\n")
    COLLECTION_NAME = 'acquisitions'
    END_POINT = 'searches/acquisitions'

    YESTURDAY_DATE = utils.get_yesterday_date()

    QUERY = {
        "field_ids": [
            "acquiree_categories",
            "acquiree_funding_total",
            "acquiree_identifier",
            "acquiree_last_funding_type",
            "acquiree_locations",
            "acquiree_num_funding_rounds",
            "acquiree_revenue_range",
            "acquiree_short_description",
            "acquirer_categories",
            "acquirer_funding_stage",
            "acquirer_funding_total",
            "acquirer_identifier",
            "acquirer_locations",
            "acquirer_num_funding_rounds",
            "acquirer_revenue_range",
            "acquirer_short_description",
            "acquisition_type",
            "announced_on",
            "completed_on",
            "created_at",
            "disposition_of_acquired",
            "entity_def_id",
            "identifier",
            "permalink",
            "price",
            "rank_acquisition",
            "short_description",
            "status",
            "terms",
            "updated_at",
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

    # get the acquisitions collection
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

    msg = {'entity': 'acquisitions',
           'total_record_updated': fetch_records_count}
    return jsonify(msg)
