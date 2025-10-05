import boto3

dynamodb = boto3.resource("dynamodb", region_name="us-east-2")
alg_recs_table = dynamodb.Table("alg_recs")

SKYGEST_TEAM_USERS = [
    'did:plc:kkhfvbrf4me4ogph35hjx3zc',
    'did:plc:6ysaocl4wbig54tsqox4a2f5',
    'did:plc:uaadt6f5bbda6cycbmatcm3z',
    'did:plc:js2q7cnku5k5fzlclsk6eigm',
    'did:plc:wsurpl22orh7kcgubie6k5yb'
]

def get_alg_recs(user_did):
    if user_did not in SKYGEST_TEAM_USERS:
        return []
   
    response = alg_recs_table.get_item(
        Key={
            "user_did": user_did
        }
    )
    if 'Item' not in response:
        return None
    
    return response.get("Item")['recs']