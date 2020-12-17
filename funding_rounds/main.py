from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
import utils


def funding_rounds(event, context):
    """
        To fetch and update the Funding Rounds entity.
    """

    print("\n-------Getting Funding Rounds entities-------\n")
    
    COLLECTION_NAME = 'funding_rounds_entities'
    END_POINT = 'searches/funding_rounds'
    TODAY_DATE = utils.get_today_date()
    YESTURDAY_DATE = utils.get_yesterday_date()

    QUERY = {
        "field_ids": [
            "announced_on",
            "closed_on",
            "created_at",
            "entity_def_id",
            "funded_organization_categories",
            "funded_organization_description",
            "funded_organization_diversity_spotlights",
            "funded_organization_funding_stage",
            "funded_organization_funding_total",
            "funded_organization_identifier",
            "funded_organization_location",
            "funded_organization_revenue_range",
            "identifier",
            "image_id",
            "investment_stage",
            "investment_type",
            "investor_identifiers",
            "is_equity",
            "lead_investor_identifiers",
            "money_raised",
            "name",
            "num_investors",
            "num_partners",
            "permalink",
            "post_money_valuation",
            "pre_money_valuation",
            "rank_funding_round",
            "short_description",
            "target_money_raised",
            "updated_at",
            "uuid",
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

    # TODO to add this to all of the functions
    if total_count is None:
        return "Error in parsing the API response. Please check the logs."

    print("total count: ", total_count)

    # get the people collection
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


    msg = {'entity': 'funding_rounds',
           'total_record_updated': fetch_records_count}
    print("\nTask Done: ", msg)


funding_rounds(1, 2)