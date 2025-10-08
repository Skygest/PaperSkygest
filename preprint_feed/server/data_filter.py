from collections import defaultdict
from atproto import models
from server.logger import logger
from server.database_dynamo import store_post, store_likes, store_reposts, store_quoteposts, mark_post_deleted
from server.database import PostURI, db
from server.post_utils import get_search_text
from server.patterns import COMPILED_PAPER_PATTERNS, COMPILED_CONTENT_PATTERNS, PDF_EXCLUSIONS

def contains_paper_link(search_text) -> bool:
    """
    Checks if a Bluesky post contains academic paper links or PDFs, including
    paper announcements common on social media.
    
    Args:
        record: A dictionary containing post record data with 'text' and optional 'embed' fields
        
    Returns:
        bool: True if any academic paper link or PDF is found, False otherwise
    """

    for compiled_pattern in COMPILED_PAPER_PATTERNS:
        for match in compiled_pattern.finditer(search_text):
            matched_text = match.group().lower()
            
            # Check exclusions for PDFs (set lookup is faster)
            if '.pdf' in matched_text:
                if any(exclusion in matched_text for exclusion in PDF_EXCLUSIONS):
                    continue
            
            return True

    matches = 0
    for compiled_pattern in COMPILED_CONTENT_PATTERNS:
        if compiled_pattern.search(search_text):
            matches += 1
            # Return true if we find at least three academic indicators
            if matches >= 3:
                return True
    
    # # Check for explicit paper URLs
    # for pattern in paper_patterns:
    #     if re.search(pattern, search_text):
    #         return True
    
    # Look for multiple academic content indicators
    # matches = 0
    # for pattern in content_patterns:
    #     if re.search(pattern, search_text):
    #         matches += 1
    #         # Return true if we find at least two academic indicators
    #         if matches >= 2:
    #             return True
    

    
    return False

def contains_arxiv_link(record) -> bool:
    """
    Specifically checks for arXiv links in a post.
    
    Args:
        record: A dictionary containing post record data with 'text' and optional 'embed' fields
        
    Returns:
        bool: True if an arXiv link is found, False otherwise
    """
    # Safely extract text from record
    post_text = record.get('text', '').lower()
    
    # Safely extract external URI from embed data
    external_uri = ''
    embed_data = record.get('embed', {})
    if isinstance(embed_data, dict):
        external_data = embed_data.get('external', {})
        if isinstance(external_data, dict):
            external_uri = external_data.get('uri', '').lower()
    
    # Check for arXiv links in either the post text or external URI
    return 'arxiv.org' in post_text or 'arxiv.org' in external_uri

def format_event(event, event_type):
    record = event['record']
    return {
        'record_uri': event['uri'], 
        'CID': event['cid'],
        'author': event['author'],
        'created_at': record['created_at'],
        'post': record['subject']['uri'],
        'type': event_type
    }

def process_created_reposts(created_reposts):
    if len(created_reposts) > 0:
        r0 = created_reposts[0]
        logger.info(r0)

def process_created_posts(created):
    posts_to_create = []

    # Process all newly created posts
    for created_post in created:
        author = created_post['author']
        record = created_post['record']

        search_text = get_search_text(record)

        # Check if the post is paper-related
        if contains_arxiv_link(record) or contains_paper_link(search_text):
            # Handle reply data carefully using dictionary access
            reply_root = reply_parent = None
            reply_data = record.get('reply', {})
            if isinstance(reply_data, dict):
                # Extract root URI if available
                root_data = reply_data.get('root', {})
                if isinstance(root_data, dict):
                    reply_root = root_data.get('uri')
                
                # Extract parent URI if available
                parent_data = reply_data.get('parent', {})
                if isinstance(parent_data, dict):
                    reply_parent = parent_data.get('uri')
            
            # Log the found paper-related post
            logger.info(author)
            logger.info(record.get('text', ''))

            try:
                created_date_day = record.get('created_at').split('T')[0]
                print(created_date_day)
            except Exception as e:
                logger.error(f"Error parsing created_at date: {e}")


            # Create the post dictionary with all necessary fields
            post_dict = {
                'at_uri': created_post['uri'],
                'CID': created_post['cid'],
                'CreatedDate': record.get('created_at'),
                'AuthorDID': author,
                'CreatedDateDay': created_date_day,
                'ReplyParent': reply_parent,
                'ReplyRoot': reply_root,
                'SearchText': search_text
            }
            
            posts_to_create.append(post_dict)

    # Store all paper-related posts in the database
    if posts_to_create:
        for post_dict in posts_to_create:
            store_post(post_dict)

        with db.atomic():
            for post_dict in posts_to_create:
                post_uri = post_dict['at_uri']
                PostURI.create(uri=post_uri)
        logger.info(f'Added to feed: {len(posts_to_create)}')

    return len(posts_to_create)

def relevant_interaction(interaction: dict) -> bool:
    # get the post referenced by this AppBskyFeedLike interaction, and check if it is in the posts database
    uri = interaction['record']['subject']['uri']
    in_db = PostURI.select().where(PostURI.uri == uri).exists()
    if in_db:
        print(f'Interaction {interaction["uri"]} is relevant: post {uri} is in the database')
    return in_db

def relevant_repost(interaction: dict) -> bool:
    # get the post referenced by this AppBskyFeedRepost interaction, and check if it is in the posts database
    uri = interaction['record']['subject']['uri']
    in_db = PostURI.select().where(PostURI.uri == uri).exists()
    if in_db:
        print(f'Repost {interaction["uri"]} is relevant: post {uri} is in the database')
    return in_db

def relevant_quotepost(quote_uri: str) -> bool:
    in_db = PostURI.select().where(PostURI.uri == quote_uri).exists()
    if in_db:
        print(f'Quote post is relevant: post {quote_uri} is in the database')
    return in_db

def process_created_likes(created_likes):
    interactions_to_create = []
    for like_interaction in created_likes:
        if relevant_interaction(like_interaction):
            interaction_dict = {
                'user_did': like_interaction['author'],
                'post_uri': like_interaction['record']['subject']['uri'],
                'created_at': like_interaction['record']['created_at'],
                'created_at_day': like_interaction['record']['created_at'].split('T')[0],
	            'post_cid': like_interaction['cid']
            }
            interactions_to_create.append(interaction_dict)
            uri = like_interaction['uri']
            logger.info(f'Added interaction {uri}')

    store_likes(interactions_to_create)
    return len(interactions_to_create)

def process_created_reposts(created_reposts):
    reposts_to_create = []
    for repost_interaction in created_reposts:
        if relevant_repost(repost_interaction):
            interaction_dict = {
                'user_did': repost_interaction['author'],
                'post_uri': repost_interaction['record']['subject']['uri'],
                'created_at': repost_interaction['record']['created_at'],
	            'post_cid': repost_interaction['cid'],
                'repost_uri': repost_interaction['uri']
            }
            reposts_to_create.append(interaction_dict)
            uri = repost_interaction['uri']
            logger.info(f'Added interaction {uri}')

    store_reposts(reposts_to_create)
    return len(reposts_to_create)

def is_quote_post(record):
    return 'embed' in record and 'record' in record['embed'] and 'uri' in record['embed']['record']

def process_created_quoteposts(created):
    quoteposts_to_create = []
    for quotepost in created: 
        record = quotepost['record']
        if is_quote_post(record):
            quote_uri = record['embed']['record']['uri']
            if relevant_quotepost(quote_uri):
                logger.info(f'Found quotepost {quotepost["uri"]} referencing {quote_uri}')
                quotepost_dict = {
                    'at_uri': quotepost['uri'],
                    'cid': quotepost['cid'],
                    'created_date': record.get('created_at'),
                    'user_did': quotepost['author'],
                    'ref_uri': quote_uri,
                    'text': record.get('text', ''),
                }
                quoteposts_to_create.append(quotepost_dict)

    if quoteposts_to_create:
        store_quoteposts(quoteposts_to_create)
        logger.info(f'Added {len(quoteposts_to_create)} quoteposts to feed')

def process_deleted_posts(deleted):
    for deleted_post in deleted:
        uri = deleted_post['uri']

        # first, check if the post exists in the PostURI table
        if not PostURI.select().where(PostURI.uri == uri).exists():
            continue

        # if it exists, delete it from the DynamoDB table
        mark_post_deleted(uri)
        logger.info(f'Deleted post {uri} from DynamoDB')

def operations_callback(ops: defaultdict) -> None:
    """
    Processes incoming operations from the Bluesky firehose, filtering for paper-related posts.
    
    Args:
        ops: A defaultdict containing created and deleted posts to process
    """

    # feed post processing (filter to only paper posts)
    n_posts_created = process_created_posts(ops[models.ids.AppBskyFeedPost]['created'])
    
    process_created_likes(ops[models.ids.AppBskyFeedLike]['created'])
    process_created_reposts(ops[models.ids.AppBskyFeedRepost]['created'])
    process_created_quoteposts(ops[models.ids.AppBskyFeedPost]['created'])

    # deletion processing
    process_deleted_posts(ops[models.ids.AppBskyFeedPost]['deleted'])

    return n_posts_created