import json

from common.response import bad_request, created, method_not_allowed, not_found, ok

# TODO: Uncomment when DynamoDB is wired up
# import os
# import boto3
# from boto3.dynamodb.conditions import Key
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['TABLE_NAME'])


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path_params = event.get('pathParameters') or {}
    reg_number = path_params.get('reg_number')

    if reg_number:
        # /vehicles/{reg_number}
        if method == 'GET':
            return _get(reg_number)
        if method == 'PUT':
            return _update(reg_number, event)
    else:
        # /vehicles
        if method == 'GET':
            return _list(event)
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list(event):
    query_params = event.get('queryStringParameters') or {}
    owner_id = query_params.get('owner_id')

    # TODO: Query DynamoDB (optionally filter by owner_id via GSI)
    # filter_expr = Attr('owner_id').eq(owner_id) if owner_id else None
    # kwargs = {'FilterExpression': filter_expr} if filter_expr else {}
    # resp = table.query(
    #     IndexName='entity-type-index',
    #     KeyConditionExpression=Key('entity_type').eq('VEHICLE'),
    #     **kwargs,
    # )
    # vehicles = resp.get('Items', [])
    vehicles = []
    return ok({'vehicles': vehicles, 'count': len(vehicles)})


def _get(reg_number):
    # TODO: Fetch from DynamoDB
    # resp = table.get_item(Key={'pk': f'VEHICLE#{reg_number}', 'sk': 'DETAILS'})
    # vehicle = resp.get('Item')
    # if not vehicle:
    #     return not_found(f'Vehicle {reg_number} not found')
    vehicle = {'reg_number': reg_number}  # placeholder
    return ok({'vehicle': vehicle})


def _create(event):
    body = _parse_body(event)

    for field in ('reg_number', 'make', 'model', 'year', 'owner_id'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    # TODO: Store in DynamoDB
    # table.put_item(Item={
    #     'pk': f'VEHICLE#{body["reg_number"]}',
    #     'sk': 'DETAILS',
    #     'entity_type': 'VEHICLE',
    #     'reg_number': body['reg_number'],
    #     'make': body['make'],
    #     'model': body['model'],
    #     'year': int(body['year']),
    #     'owner_id': body['owner_id'],
    #     'driver_id': body.get('driver_id', ''),
    #     'created_at': _now(),
    # })

    return created({'message': 'Vehicle created', 'reg_number': body['reg_number']})


def _update(reg_number, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    # TODO: Update in DynamoDB
    # table.update_item(
    #     Key={'pk': f'VEHICLE#{reg_number}', 'sk': 'DETAILS'},
    #     UpdateExpression='SET driver_id = :d, updated_at = :ts',
    #     ExpressionAttributeValues={':d': body.get('driver_id', ''), ':ts': _now()},
    # )

    return ok({'message': 'Vehicle updated', 'reg_number': reg_number})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}
