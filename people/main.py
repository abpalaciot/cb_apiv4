from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
import utils


def people(event, context):
    """
        To fetch and update the People entity.
    """
    print("\n-------Getting people entities-------\n")
    
    END_POINT = 'searches/people'
    COLLECTION_NAME = 'people_entities'
    YESTURDAY_DATE = utils.get_yesterday_date()
    TODAY_DATE = utils.get_today_date()

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

    print("\nFetching the entities please wait. This will take sometime.")
    total_count, entities = utils.fetch_data(QUERY, END_POINT)
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
        print("inserted records: ")
        # pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        # get the last record
        
        print("------------------------")

        after_id = entities[-1].get('uuid', None)
        if after_id:
            # print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()


    msg = {'entity': 'Poeple', 'total_record_updated': fetch_records_count}
    print("\nTask Done: ", msg)
