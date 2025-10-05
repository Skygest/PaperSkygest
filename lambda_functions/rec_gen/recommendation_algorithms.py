import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class AlgorithmData:
    def __init__(self, follows_posts, follows_reposts=None, follows_quoteposts=None):
        self.follows_posts = follows_posts
        self.follows_reposts = follows_reposts if follows_reposts is not None else []
        self.follows_quoteposts = follows_quoteposts if follows_quoteposts is not None else []

def process_post(post):
    if 'reason' in post:
        return {'post': post['post'], 'reason': post['reason']}
    else:
        return post['post']

def sort_posts(posts):
    posts.sort(key=lambda x: x['createdDate'], reverse=True)
    return posts

def follow_control(data: AlgorithmData):
    return build_chron_recs(data.follows_posts)

def follow_reposts(data: AlgorithmData):
    return build_chron_recs(data.follows_posts + data.follows_reposts)

def follow_quoteposts(data: AlgorithmData):
    return build_chron_recs(data.follows_posts + data.follows_quoteposts)

def follow_all(data: AlgorithmData):
    return build_chron_recs(data.follows_posts + data.follows_reposts + data.follows_quoteposts)

def build_chron_recs(posts):
    posts = sort_posts(posts)
    return [process_post(post) for post in posts], posts
