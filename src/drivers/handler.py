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
    driver_id = path_params.get('id')
    logger.info('Request: %s /drivers%s', method, f'/{driver_id}' if driver_id else '')

    if driver_id:
        if method == 'GET':
            return _get(driver_id)
        if method == 'PUT':
            return _update(driver_id, event)
        if method == 'DELETE':
            return _delete(driver_id)
    else:
        if method == 'GET':
            return _list()
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list():
    try:
        resp = table.scan(FilterExpression=Attr('entity_type').eq('DRIVER'))
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    drivers = resp.get('Items', [])
    return ok({'drivers': drivers, 'count': len(drivers)})


def _get(driver_id):
    try:
        resp = table.get_item(Key={'PK': f'DRIVER#{driver_id}', 'SK': 'PROFILE'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    driver = resp.get('Item')
    if not driver:
        logger.warning('Driver not found: driver_id=%s', driver_id)
        return not_found(f'Driver {driver_id} not found')

    return ok({'driver': driver})


def _create(event):
    body = _parse_body(event)

    for field in ('name', 'id_number', 'license_number', 'phone'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    driver_id = str(uuid.uuid4())
    logger.info('Creating driver: driver_id=%s', driver_id)

    try:
        table.put_item(Item={
            'PK': f'DRIVER#{driver_id}',
            'SK': 'PROFILE',
            'entity_type': 'DRIVER',
            'driver_id': driver_id,
            'name': body['name'],
            'id_number': body['id_number'],
            'license_number': body['license_number'],
            'license_expiry': body.get('license_expiry', ''),
            'phone': body['phone'],
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Driver created: driver_id=%s', driver_id)
    return created({'message': 'Driver created', 'driver_id': driver_id})


def _update(driver_id, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    logger.info('Updating driver: driver_id=%s', driver_id)

    try:
        table.update_item(
            Key={'PK': f'DRIVER#{driver_id}', 'SK': 'PROFILE'},
            UpdateExpression='SET phone = :p, license_expiry = :le, updated_at = :ts',
            ExpressionAttributeValues={
                ':p': body.get('phone'),
                ':le': body.get('license_expiry', ''),
                ':ts': _now(),
            },
        )
    except ClientError as e:
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Driver updated: driver_id=%s', driver_id)
    return ok({'message': 'Driver updated', 'driver_id': driver_id})


def _delete(driver_id):
    logger.info('Deleting driver: driver_id=%s', driver_id)

    try:
        table.delete_item(Key={'PK': f'DRIVER#{driver_id}', 'SK': 'PROFILE'})
    except ClientError as e:
        logger.error('DynamoDB delete_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Driver deleted: driver_id=%s', driver_id)
    return ok({'message': 'Driver deleted', 'driver_id': driver_id})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
