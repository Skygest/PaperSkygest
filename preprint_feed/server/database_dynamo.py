#!/usr/bin/env python3
import boto3
from server.logger import logger
from botocore.exceptions import ClientError
import time
from datetime import datetime, timezone
import gc

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
posts_table = dynamodb.Table('paper_posts')
interactions_table = dynamodb.Table('interactions')
reposts_table = dynamodb.Table('reposts')
quoteposts_table = dynamodb.Table('quoteposts')

def mark_post_deleted(post_uri):
    """Mark a post as deleted in the DynamoDB table, and add a deletion timestamp"""
    try:
        posts_table.update_item(
            Key={'at_uri': post_uri},
            UpdateExpression='SET #status = :val, #deleted_at = :timestamp',
            ExpressionAttributeNames={'#status': 'status', '#deleted_at': 'deleted_at'},
            ExpressionAttributeValues={':val': 'deleted', ':timestamp': datetime.now(timezone.utc).isoformat()}
        )
        logger.info(f'Marked post {post_uri} as deleted')
    except ClientError as e:
        logger.error(f"Error marking post {post_uri} as deleted: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        gc.collect()

def store_post(post):
    """Store post in DynamoDB"""
    try:
        posts_table.put_item(Item=post)
    except ClientError as e:
        post_uri = post['at_uri']
        logger.error(f"DynamoDB error storing post {post_uri}: {str(e)}")
        if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            time.sleep(1)  # Basic backoff
    except Exception as e:
        logger.error(f"Error storing post: {str(e)}")
    finally:
        # Force garbage collection
        gc.collect()    

def store_likes(interaction_dicts):
    """Store like in DynamoDB"""
    with interactions_table.batch_writer() as batch:
        # Add each item to the batch
        if len(interaction_dicts) > 1:
            print([interaction['post_uri'] for interaction in interaction_dicts])
            print([interaction['user_did'] for interaction in interaction_dicts])
        for item in interaction_dicts:
            try:
                batch.put_item(Item=item)
            except ClientError as e:
                user = item['user_did']
                post_uri = item['post_uri']
                logger.error(f"DynamoDB error storing interaction where user {user} liked post {post_uri}: {str(e)}")
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    time.sleep(1)  # Basic backoff
            except Exception as e:
                logger.error(f"Error storing post: {str(e)}")
            finally:
                # Force garbage collection
                gc.collect()
    

def store_reposts(interaction_dicts):
    """Store reposts in DynamoDB"""
    with reposts_table.batch_writer() as batch:
        # Add each item to the batch
        for item in interaction_dicts:
            if len(interaction_dicts) > 1:
                print(interaction_dicts)
            try:
                batch.put_item(Item=item)
            except ClientError as e:
                user_did = item['user_did']
                post_uri = item['post_uri']
                logger.error(f"DynamoDB error storing repost of post {post_uri} by user {user_did}: {str(e)}")
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    time.sleep(1)  # Basic backoff
            except Exception as e:
                logger.error(f"Error storing post: {str(e)}")
            finally:
                # Force garbage collection
                gc.collect()

def store_quoteposts(quotepost_dicts):
    """Store quoteposts in DynamoDB"""
    with quoteposts_table.batch_writer() as batch:
        for quotepost_dict in quotepost_dicts:
            try:
                batch.put_item(Item=quotepost_dict)
            except ClientError as e:
                quotepost_uri = quotepost_dict['at_uri']
                logger.error(f"DynamoDB error storing quotepost {quotepost_uri}: {str(e)}")
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    time.sleep(1)  # Basic backoff
            except Exception as e:
                logger.error(f"Error storing quotepost: {str(e)}")
            finally:
                # Force garbage collection
                gc.collect()