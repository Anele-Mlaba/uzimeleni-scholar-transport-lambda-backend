import json
import uuid

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
    driver_id = path_params.get('id')

    if driver_id:
        # /drivers/{id}
        if method == 'PUT':
            return _update(driver_id, event)
    else:
        # /drivers
        if method == 'GET':
            return _list()
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list():
    # TODO: Query DynamoDB
    # resp = table.query(
    #     IndexName='entity-type-index',
    #     KeyConditionExpression=Key('entity_type').eq('DRIVER'),
    # )
    # drivers = resp.get('Items', [])
    drivers = []
    return ok({'drivers': drivers, 'count': len(drivers)})


def _create(event):
    body = _parse_body(event)

    for field in ('name', 'id_number', 'license_number', 'phone'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    driver_id = str(uuid.uuid4())

    # TODO: Store in DynamoDB
    # table.put_item(Item={
    #     'pk': f'DRIVER#{driver_id}',
    #     'sk': 'PROFILE',
    #     'entity_type': 'DRIVER',
    #     'driver_id': driver_id,
    #     'name': body['name'],
    #     'id_number': body['id_number'],
    #     'license_number': body['license_number'],
    #     'license_expiry': body.get('license_expiry', ''),
    #     'phone': body['phone'],
    #     'created_at': _now(),
    # })

    return created({'message': 'Driver created', 'driver_id': driver_id})


def _update(driver_id, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    # TODO: Update in DynamoDB
    # table.update_item(
    #     Key={'pk': f'DRIVER#{driver_id}', 'sk': 'PROFILE'},
    #     UpdateExpression='SET phone = :p, license_expiry = :le, updated_at = :ts',
    #     ExpressionAttributeValues={
    #         ':p': body.get('phone'),
    #         ':le': body.get('license_expiry', ''),
    #         ':ts': _now(),
    #     },
    # )

    return ok({'message': 'Driver updated', 'driver_id': driver_id})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}
