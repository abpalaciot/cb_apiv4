from pprint import pprint
from pymongo import MongoClient, ASCENDING, TEXT, ReplaceOne
from pymongo.errors import BulkWriteError
from flask import jsonify
import utils


def organizations(request):
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

        inserted = org_col.insert_many(entities)
        fetch_records_count += len(entities)
        print("inserted records: ")
        pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        # get the last record
        
        print("------------------------")

        after_id = entities[-1].get('uuid', None)
        if after_id:
            print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()

    msg = {'entity': 'Organization', 'total_record_updated': fetch_records_count}
    return jsonify(msg)


def people(request):
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

        inserted = col.insert_many(entities)
        fetch_records_count += len(entities)
        print("inserted records: ")
        pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        # get the last record
        
        print("------------------------")

        after_id = entities[-1].get('uuid', None)
        if after_id:
            print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()


    msg = {'entity': 'Poeple', 'total_record_updated': fetch_records_count}
    return jsonify(msg)


def funding_rounds(request):
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

        inserted = col.insert_many(entities)
        fetch_records_count += len(entities)
        print("inserted records: ")
        pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        print("------------------------")

        # get the last record
        after_id = entities[-1].get('uuid', None)
        if after_id:
            print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()


    msg = {'entity': 'funding_rounds',
           'total_record_updated': fetch_records_count}
    return jsonify(msg)


def acquisitions(request):
    """
        To fetch and update the Acquisitions entity.
    """

    print("\n-------Getting Acquisitions entities-------\n")
    COLLECTION_NAME = 'acquisitions_entities'
    END_POINT = 'searches/acquisitions'
    TODAY_DATE = utils.get_today_date()
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

        inserted = col.insert_many(entities)
        fetch_records_count += len(entities)
        print("inserted records: ")
        pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        print("------------------------")

        # get the last record
        after_id = entities[-1].get('uuid', None)
        if after_id:
            print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()

    msg = {'entity': 'acquisitions',
           'total_record_updated': fetch_records_count}
    return jsonify(msg)


def press_references(request):
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

        inserted = col.insert_many(entities)
        fetch_records_count += len(entities)
        print("inserted records: ")
        pprint(inserted.inserted_ids)
        print("total_count: ", total_count,
              ", fetched records: ", fetch_records_count)

        print("------------------------")

        # get the last record
        after_id = entities[-1].get('uuid', None)
        if after_id:
            print("Get next batch after id: ", after_id)
            # print("Entities len: ", )
            QUERY['after_id'] = after_id
        entities.clear()

    msg = {'entity': 'press_references',
           'total_record_updated': fetch_records_count}
    return jsonify(msg)
