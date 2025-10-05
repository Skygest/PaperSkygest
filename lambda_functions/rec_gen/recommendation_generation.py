import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key, Attr
import time
import logging
import os
from follows import get_all_follows
from recommendation_algorithms import follow_control, follow_quoteposts, follow_reposts, follow_all, AlgorithmData
from save_counterfactuals import save_counterfactuals

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
recommendations_table = dynamodb.Table('recommendations')
posts_table = dynamodb.Table('paper_posts')
access_agg_table = dynamodb.Table('user_accesses_agg')
reposts_table = dynamodb.Table('reposts')
quoteposts_table = dynamodb.Table('quoteposts')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FOLLOW_RECOMMENDER_NAME = 'recent-following-posts'
FOLLOW_RECOMMENDER_NAME_REPOSTS = 'recent-following-reposts'
FOLLOW_RECOMMENDER_NAME_QUOTEPOSTS = 'recent-following-quoteposts'
FOLLOW_RECOMMENDER_NAME_REPOSTS_QUOTEPOSTS = 'recent-following-repostsandquoteposts'

ALGORITHMS_DICT = {
    FOLLOW_RECOMMENDER_NAME: follow_control,
    FOLLOW_RECOMMENDER_NAME_REPOSTS: follow_reposts,
    FOLLOW_RECOMMENDER_NAME_QUOTEPOSTS: follow_quoteposts,
    FOLLOW_RECOMMENDER_NAME_REPOSTS_QUOTEPOSTS: follow_all
}

AGGREGATE_FEED_ID = 'aggregate_feed'

FIXED_POSTS = {
    'no_feed': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3ljjnqm4vl72l',
    'few_accesses': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3ljvzkxtotk2r' ,
    'consent': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3lpozj5qvcs24',
    'full_announcement': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3lppahmc72k2j'
}

FEED_LIMIT = 150
FEED_MIN = 10

ACCESS_THRESHOLD = 10
CONSENT_THRESHOLD = 5

def get_agg_feed():
    response= recommendations_table.get_item(Key={
            'user_did': AGGREGATE_FEED_ID,
            'recommender': AGGREGATE_FEED_ID
        })

    if response and response.get('Item'):
        return response['Item']['recommendations']
    return []

def create_repost_object(post):
    obj = {
        'post': post['post_uri'],
        'createdDate': post['created_at']
    }
    if 'repost_uri' in post and post['repost_uri'] is not None:
        obj['reason'] = {
            '$type': 'app.bsky.feed.defs#skeletonReasonRepost',
            'repost': post.get('repost_uri')
        }
    return obj

def get_author_recent_reposts(author_did):
    """Get 10 most recent posts for a single author"""
    response = reposts_table.query(
        IndexName='user_did-created_at-index',
        KeyConditionExpression=Key('user_did').eq(author_did),
        Limit=10,
        ScanIndexForward=False
    )
    return [create_repost_object(post) for post in response['Items']]

def get_author_recent_quoteposts(author_did):
    """Get 10 most recent quoteposts for a single author"""
    response = quoteposts_table.query(
        IndexName='user_did-created_date-index',
        KeyConditionExpression=Key('user_did').eq(author_did),
        Limit=10,
        ScanIndexForward=False
    )
    return [{
        'post': post['at_uri'],
        'createdDate': post['created_date']
    } for post in response['Items']]

def get_author_recent_posts(author_did, table):
    """Get 10 most recent posts for a single author, filtering out deleted posts"""
    response = table.query(
        IndexName='AuthorDID-CreatedDate-index',
        KeyConditionExpression=Key('AuthorDID').eq(author_did),
        Limit=10,
        FilterExpression=Attr('status').ne('deleted'),
        ScanIndexForward=False
    )
    return [{
        'post': post['at_uri'],
        'createdDate': post['CreatedDate']
    } for post in response['Items']]

def process_post(post):
    if 'reason' in post:
        return {'post': post['post'], 'reason': post['reason']}
    else:
        return post['post']

def get_follows_data(following_dids):
    all_posts = []
    for author_did in following_dids:
        all_posts.extend(get_author_recent_posts(author_did, posts_table))

    all_reposts = []
    for author_did in following_dids:
        all_reposts.extend(get_author_recent_reposts(author_did))

    all_quoteposts = []
    for author_did in following_dids:
        all_quoteposts.extend(get_author_recent_quoteposts(author_did))

    return AlgorithmData(
        follows_posts=all_posts,
        follows_reposts=all_reposts,
        follows_quoteposts=all_quoteposts
    )

def get_all_authors_posts(following_dids, include_reposts=False, include_quoteposts=False):
    """Get 10 most recent posts for each author"""
    start_time = time.time()
    all_posts = []

    for author_did in following_dids:
        all_posts.extend(get_author_recent_posts(author_did, posts_table))

    if include_reposts:
        all_reposts = []
        for author_did in following_dids:
            all_reposts.extend(get_author_recent_reposts(author_did))
        all_posts.extend(all_reposts)

    if include_quoteposts:
        all_quoteposts = []
        for author_did in following_dids:
            all_quoteposts.extend(get_author_recent_quoteposts(author_did))
        all_posts.extend(all_quoteposts)


    if include_quoteposts and include_reposts:
        data = AlgorithmData(all_posts, all_reposts, all_quoteposts)

        logger.info('Including reposts and quoteposts')
        return ALGORITHMS_DICT[FOLLOW_RECOMMENDER_NAME_REPOSTS_QUOTEPOSTS](data)

    # Sort all posts by date
    all_posts.sort(key=lambda x: x['createdDate'], reverse=True)
    logger.info(f'Retrieved {len(all_posts)} posts')
    logger.info(f'Time for retrieval: {time.time() - start_time}')
    return [process_post(post) for post in all_posts], all_posts

def low_accesses(accesses):
    logger.info('Checking for low accesses')
    if accesses:
        return accesses['access_count'] < ACCESS_THRESHOLD
    return False

def consent_accesses(accesses):
    logger.info('Checking whether to include consent thread')
    if accesses:
        consent_accesses = accesses['consent_accesses'] if 'consent_accesses' in accesses else 0
        return consent_accesses <= CONSENT_THRESHOLD
    return False

def get_access_details(user_did):
    response = access_agg_table.get_item(Key={
        'user_did': user_did
    })
    return response.get('Item', None)

def construct_recs_object(recommender_name, recommendations, user_did, default_from, start_time, end_time):
    return {
        'user_did': user_did, 
        'recommender': recommender_name, 
        'recommendations': recommendations, 
        'default_from': default_from, 
        'generation_date': datetime.now(timezone.utc).isoformat(), 
        'time_to_generate': int(round(end_time - start_time))
    }

def save_recs(batch, recommender_name, recommendations, user_did, default_from, start_time, end_time):
    try:
        recs_object = construct_recs_object(recommender_name, recommendations, user_did, default_from, start_time, end_time)
        if recs_object:
            batch.put_item(Item=recs_object)
        return recs_object
    except Exception as e:
        logger.error(f'Error saving recs for user {user_did}: {e}')

def get_prefix_posts(user_did):
    recommendations = []
    accesses = get_access_details(user_did)
    if low_accesses(accesses):
        recommendations = [FIXED_POSTS['few_accesses'], FIXED_POSTS['consent']]
    elif consent_accesses(accesses):
        recommendations.append(FIXED_POSTS['full_announcement'])

    return recommendations

def get_suffix_posts(user_did, recommendations, agg_feed):
    default_from = -1
    if len(recommendations) < FEED_MIN:
        logger.info(f'Only {len(recommendations)} recs for user {user_did}, appending agg feed')
        default_from = len(recommendations)
        recommendations.append(FIXED_POSTS['no_feed'])
        recommendations.extend(agg_feed[:FEED_LIMIT - len(recommendations)])

    return recommendations, default_from

def generate_recommendations_user_updated(batch, user_did, agg_feed, retry_n, scheduled_generation=True):
    start_time = time.time()
    recommendation_prefix = get_prefix_posts(user_did)
    
    following_dids = get_all_follows(user_did, retry_n)
    if following_dids is not None:
        following_dids.append(user_did)

        data = get_follows_data(following_dids)
        end_time = time.time()

        recommendations_dicts = []
        for alg in ALGORITHMS_DICT:
            recommendations = recommendation_prefix.copy()
            recs, recs_raw = ALGORITHMS_DICT[alg](data)
            if alg == FOLLOW_RECOMMENDER_NAME:
                recs_raw_follows = recs_raw
            recommendations.extend(recs)
            recommendations = recommendations[:FEED_LIMIT] if len(recommendations) > FEED_LIMIT else recommendations
            recommendations, default_from = get_suffix_posts(user_did, recommendations, agg_feed)
            recommendations_dict = save_recs(batch, alg, recommendations, user_did, default_from, start_time, end_time)
            recommendations_dicts.append(recommendations_dict)

        if scheduled_generation:
            save_counterfactuals(user_did, recommendations_dicts)

        return recs_raw_follows
    else:
        return None

def generate_recommendations_agg(all_raw_recs):
    all_raw_recs.sort(key=lambda x: x['createdDate'], reverse=True)
    agg_recs = all_raw_recs[:FEED_LIMIT]
    agg_posts = [post['post'] for post in agg_recs]
    return {'user_did': AGGREGATE_FEED_ID, 'recommender': AGGREGATE_FEED_ID, 'recommendations': agg_posts, 'generation_date': datetime.now(timezone.utc).isoformat()}

def generate_recommendations_users(users, generate_agg, retry_n, scheduled_generation=True):
    logger.info(f"Generating recommendations for {len(users)} users")
    agg_recs = get_agg_feed()

    with recommendations_table.batch_writer() as batch:
        all_recs = []
        for user in users:
            recs_raw = generate_recommendations_user_updated(batch, user, agg_recs, retry_n, scheduled_generation)
            if recs_raw:
                all_recs.extend(recs_raw)
        
        if generate_agg:
            agg_user_recs = generate_recommendations_agg(all_recs)
            recommendations_table.put_item(Item=agg_user_recs)
            