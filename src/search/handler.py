from common.response import bad_request, method_not_allowed, ok

# TODO: Uncomment when DynamoDB is wired up
# import os
# import boto3
# from boto3.dynamodb.conditions import Key
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['TABLE_NAME'])


def lambda_handler(event, context):
    if event.get('httpMethod', '') != 'GET':
        return method_not_allowed()

    return _search(event)


# ── Handlers ──────────────────────────────────────────────────────────────────

def _search(event):
    """
    Supports two mutually exclusive query parameters:
      GET /search?reg_number=GP123456  → search vehicles by registration number
      GET /search?id_number=8901015009087 → search owners/drivers by SA ID number
    """
    query_params = event.get('queryStringParameters') or {}
    reg_number = query_params.get('reg_number', '').strip()
    id_number = query_params.get('id_number', '').strip()

    if not reg_number and not id_number:
        return bad_request('Provide reg_number or id_number as a query parameter')

    if reg_number:
        return _by_reg_number(reg_number)
    return _by_id_number(id_number)


def _by_reg_number(reg_number):
    """
    Look up a vehicle (and its linked owner/driver) by registration number.
    Requires a GSI on reg_number in DynamoDB.
    """
    # TODO: Query DynamoDB GSI
    # resp = table.query(
    #     IndexName='reg_number-index',
    #     KeyConditionExpression=Key('reg_number').eq(reg_number.upper()),
    # )
    # results = resp.get('Items', [])
    results = []  # placeholder

    return ok({
        'query': {'reg_number': reg_number},
        'results': results,
        'count': len(results),
    })


def _by_id_number(id_number):
    """
    Look up an owner or driver by South African ID number.
    Requires a GSI on id_number in DynamoDB.
    """
    # TODO: Query DynamoDB GSI (covers both OWNER and DRIVER entity types)
    # resp = table.query(
    #     IndexName='id_number-index',
    #     KeyConditionExpression=Key('id_number').eq(id_number),
    # )
    # results = resp.get('Items', [])
    results = []  # placeholder

    return ok({
        'query': {'id_number': id_number},
        'results': results,
        'count': len(results),
    })
