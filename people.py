from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
from flask import jsonify
import utils


def people(request):
    """
        To fetch and update the People entity.
    """
    print("\n-------Getting people entities-------\n")
    COLLECTION_NAME = 'people'
    END_POINT = 'searches/people'

    YESTURDAY_DATE = utils.get_yesterday_date()

    QUERY = {
        "field_ids": [
            "aliases",
            "born_on",
            "created_at",
            "description",
            "died_on",
            "entity_def_id",
            "facebook",
            "facet_ids",
            "first_name",
            "gender",
            "identifier",
            "image_id",
            "image_url",
            "investor_stage",
            "investor_type",
            "last_name",
            "layout_id",
            "linkedin",
            "location_group_identifiers",
            "location_identifiers",
            "middle_name",
            "name",
            "num_articles",
            "num_current_advisor_jobs",
            "num_current_jobs",
            "num_diversity_spotlight_investments",
            "num_event_appearances",
            "num_exits",
            "num_exits_ipo",
            "num_founded_organizations",
            "num_investments",
            "num_jobs",
            "num_lead_investments",
            "num_partner_investments",
            "num_past_advisor_jobs",
            "num_past_jobs",
            "num_portfolio_organizations",
            "override_layout_id",
            "permalink",
            "permalink_aliases",
            "primary_job_title",
            "primary_organization",
            "rank_delta_d30",
            "rank_delta_d7",
            "rank_delta_d90",
            "rank_person",
            "rank_principal",
            "short_description",
            "twitter",
            "updated_at",
            "uuid",
            "website",
            "website_url",
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
                "field_id": "rank_person",
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

    # get the people collection
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

    msg = {'entity': 'Poeple', 'total_record_updated': fetch_records_count}
    return jsonify(msg)
