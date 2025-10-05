import boto3
from datetime import datetime, timezone
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
users_table = dynamodb.Table('users')
counterfactuals_table = dynamodb.Table('counterfactual_recs')

def is_research_user(user):
    return not user.get('research_remove', False)

def check_research_user(user_did):
    response = users_table.get_item(Key={'user_did': user_did})
    user = response.get('Item')
    return is_research_user(user)

def get_first_page_recs(user_did, recommendations_dicts, save_date, page_length=30):
    recs_to_save = []
    for recommender in recommendations_dicts:
        first_page = recommender['recommendations'][:page_length]
        rec_to_save = {
            'uuid': str(uuid.uuid4()),
            'user_did': user_did,
            'recommender_did': recommender['recommender'],
            'recommendations_page': first_page,
            'default_from': recommender['default_from'],
            'generation_date': recommender['generation_date'],
            'save_date': save_date
        }
        recs_to_save.append(rec_to_save)
    return recs_to_save

def save_counterfactuals(user_did, recommendation_dicts):
    save_date = datetime.now(timezone.utc).isoformat()

    is_research_user = check_research_user(user_did)

    if is_research_user:
        logger.info(f"Saving counterfactuals for user {user_did}")
        recs_to_save = get_first_page_recs(user_did, recommendation_dicts, save_date)
        with counterfactuals_table.batch_writer() as ct:
            for rec_to_save in recs_to_save:
                ct.put_item(Item=rec_to_save)