from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
import utils


def organizations(event, context):
    """
        To fetch and update the Organizations entity
    """
    print("\n-------Getting Organizations entities-------\n")
    END_POINT = 'searches/organizations'

    YESTURDAY_DATE = utils.get_yesterday_date()
    TODAY_DATE = utils.get_today_date()
    COLLECTION_NAME = 'organization_entities'


    QUERY = {
        "field_ids": [
            "acquirer_identifier",
            "aliases",
            "categories",
            "category_groups",
            "closed_on",
            "company_type",
            "contact_email",
            "created_at",
            "delisted_on",
            "demo_days",
            "description",
            "diversity_spotlights",
            "entity_def_id",
            "equity_funding_total",
            "exited_on",
            "facebook",
            "facet_ids",
            "founded_on",
            "founder_identifiers",
            "funding_stage",
            "funding_total",
            "funds_total",
            "hub_tags",
            "identifier",
            "image_id",
            "image_url",
            "investor_identifiers",
            "investor_stage",
            "investor_type",
            "ipo_status",
            "last_equity_funding_total",
            "last_equity_funding_type",
            "last_funding_at",
            "last_funding_total",
            "last_funding_type",
            "layout_id",
            "legal_name",
            "linkedin",
            "listed_stock_symbol",
            "location_group_identifiers",
            "location_identifiers",
            "name",
            "num_acquisitions",
            "num_alumni",
            "num_articles",
            "num_current_advisor_positions",
            "num_current_positions",
            "num_diversity_spotlight_investments",
            "num_employees_enum",
            "num_enrollments",
            "num_event_appearances",
            "num_exits",
            "num_exits_ipo",
            "num_founder_alumni",
            "num_founders",
            "num_funding_rounds",
            "num_funds",
            "num_investments",
            "num_investors",
            "num_lead_investments",
            "num_lead_investors",
            "num_past_positions",
            "num_portfolio_organizations",
            "num_sub_organizations",
            "operating_status",
            "override_layout_id",
            "owner_identifier",
            "permalink",
            "permalink_aliases",
            "phone_number",
            "program_application_deadline",
            "program_duration",
            "program_type",
            "rank_delta_d30",
            "rank_delta_d7",
            "rank_delta_d90",
            "rank_org",
            "rank_principal",
            "revenue_range",
            "school_method",
            "school_program",
            "school_type",
            "short_description",
            "status",
            "stock_exchange_symbol",
            "stock_symbol",
            "twitter",
            "updated_at",
            "uuid",
            "valuation",
            "valuation_date",
            "website",
            "website_url",
            "went_public_on"
        ],
        "order": [
            {
                "field_id": "rank_org",
                "sort": "asc"
            }
        ],
        "query": [
            {
                "type": "predicate",
                "field_id": "updated_at",
                "operator_id": "gte",
                "values": [str(YESTURDAY_DATE)]
            },
        ],
        "limit": 1000
    }

    print("\nFetching the entities please wait. This will take sometime.")
    total_count, entities = utils.fetch_data(QUERY, END_POINT)
    if total_count is None:
        return "Error in parsing the API response. Please check the logs."

    print("total count: ", total_count)

    # get the organization collection
    org_col = utils.get_mongodb_collection(COLLECTION_NAME)

    fetch_records_count = 0

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

        org_col.insert_many(entities)
        fetch_records_count += len(entities)
        # print("inserted records: ")
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

    msg = {'entity': 'Organization', 'total_record_updated': fetch_records_count}
    print("\nTask Done: ", msg)

