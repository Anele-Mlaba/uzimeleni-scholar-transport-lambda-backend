import json
import logging
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.response import bad_request, created, method_not_allowed, not_found, ok, server_error

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('zimeleni-transport')


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path_params = event.get('pathParameters') or {}
    reg_number = path_params.get('reg_number')
    logger.info('Request: %s /vehicles%s', method, f'/{reg_number}' if reg_number else '')

    if reg_number:
        if method == 'GET':
            return _get(reg_number)
        if method == 'PUT':
            return _update(reg_number, event)
        if method == 'DELETE':
            return _delete(reg_number)
    else:
        if method == 'GET':
            return _list(event)
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list(event):
    query_params = event.get('queryStringParameters') or {}
    owner_id = query_params.get('owner_id')

    filter_expr = Attr('entity_type').eq('VEHICLE')
    if owner_id:
        filter_expr = filter_expr & Attr('owner_id').eq(owner_id)

    try:
        resp = table.scan(FilterExpression=filter_expr)
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    vehicles = resp.get('Items', [])
    return ok({'vehicles': vehicles, 'count': len(vehicles)})


def _get(reg_number):
    try:
        resp = table.get_item(Key={'PK': f'VEHICLE#{reg_number}', 'SK': 'DETAILS'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    vehicle = resp.get('Item')
    if not vehicle:
        logger.warning('Vehicle not found: reg_number=%s', reg_number)
        return not_found(f'Vehicle {reg_number} not found')

    return ok({'vehicle': vehicle})


def _create(event):
    body = _parse_body(event)

    for field in ('reg_number', 'make', 'model', 'year', 'owner_id'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    logger.info('Creating vehicle: reg_number=%s', body['reg_number'])

    try:
        table.put_item(Item={
            'PK': f'VEHICLE#{body["reg_number"]}',
            'SK': 'DETAILS',
            'entity_type': 'VEHICLE',
            'reg_number': body['reg_number'],
            'make': body['make'],
            'model': body['model'],
            'year': int(body['year']),
            'owner_id': body['owner_id'],
            'driver_id': body.get('driver_id', ''),
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Vehicle created: reg_number=%s', body['reg_number'])
    return created({'message': 'Vehicle created', 'reg_number': body['reg_number']})


def _update(reg_number, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    logger.info('Updating vehicle: reg_number=%s', reg_number)

    try:
        table.update_item(
            Key={'PK': f'VEHICLE#{reg_number}', 'SK': 'DETAILS'},
            UpdateExpression='SET driver_id = :d, updated_at = :ts',
            ExpressionAttributeValues={':d': body.get('driver_id', ''), ':ts': _now()},
        )
    except ClientError as e:
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Vehicle updated: reg_number=%s', reg_number)
    return ok({'message': 'Vehicle updated', 'reg_number': reg_number})


def _delete(reg_number):
    logger.info('Deleting vehicle: reg_number=%s', reg_number)

    try:
        table.delete_item(Key={'PK': f'VEHICLE#{reg_number}', 'SK': 'DETAILS'})
    except ClientError as e:
        logger.error('DynamoDB delete_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Vehicle deleted: reg_number=%s', reg_number)
    return ok({'message': 'Vehicle deleted', 'reg_number': reg_number})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
