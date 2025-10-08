from datetime import datetime

import peewee

db = peewee.SqliteDatabase('feed_database.db')


class BaseModel(peewee.Model):
    class Meta:
        database = db

class Post(BaseModel):
    uri = peewee.CharField(index=True)
    cid = peewee.CharField()
    reply_parent = peewee.CharField(null=True, default=None)
    reply_root = peewee.CharField(null=True, default=None)
    indexed_at = peewee.DateTimeField(default=datetime.utcnow)


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.BigIntegerField()


class PostURI(BaseModel):
    uri = peewee.CharField(index=True)

if db.is_closed():
    db.connect()

    # Get existing tables
    existing_tables = db.get_tables()
    
    # Drop the old Post table if it exists
    if 'post' in existing_tables:
        db.execute_sql('DROP TABLE IF EXISTS post;')
    
    # Create tables (safe=True means it won't recreate existing tables)
    db.create_tables([PostURI, SubscriptionState], safe=True)