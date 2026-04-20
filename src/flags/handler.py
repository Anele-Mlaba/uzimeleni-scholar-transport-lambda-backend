import json
import logging
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.response import bad_request, created, method_not_allowed, ok, server_error

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('zimeleni-transport')

VALID_ENTITY_TYPES = ('owner', 'driver', 'vehicle')
VALID_SEVERITIES = ('low', 'medium', 'high')


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    logger.info('Request: %s /flags', method)

    if method == 'GET':
        return _list(event)
    if method == 'POST':
        return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list(event):
    query_params = event.get('queryStringParameters') or {}
    entity_id = query_params.get('entity_id')
    entity_type = query_params.get('entity_type')
    severity = query_params.get('severity')

    filter_expr = Attr('entity_type').eq('FLAG')
    if entity_id:
        filter_expr = filter_expr & Attr('entity_id').eq(entity_id)
    if entity_type:
        filter_expr = filter_expr & Attr('flagged_entity_type').eq(entity_type)
    if severity:
        filter_expr = filter_expr & Attr('severity').eq(severity)

    try:
        resp = table.scan(FilterExpression=filter_expr)
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    flags = resp.get('Items', [])
    return ok({'flags': flags, 'count': len(flags)})


def _create(event):
    body = _parse_body(event)

    for field in ('entity_id', 'entity_type', 'reason'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    if body['entity_type'] not in VALID_ENTITY_TYPES:
        return bad_request(f'entity_type must be one of: {", ".join(VALID_ENTITY_TYPES)}')

    severity = body.get('severity', 'medium')
    if severity not in VALID_SEVERITIES:
        return bad_request(f'severity must be one of: {", ".join(VALID_SEVERITIES)}')

    flag_id = str(uuid.uuid4())
    logger.info('Creating flag: flag_id=%s entity_id=%s', flag_id, body['entity_id'])

    try:
        table.put_item(Item={
            'PK': f'FLAG#{flag_id}',
            'SK': 'DETAILS',
            'entity_type': 'FLAG',
            'flag_id': flag_id,
            'entity_id': body['entity_id'],
            'flagged_entity_type': body['entity_type'],
            'reason': body['reason'],
            'severity': severity,
            'resolved': False,
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Flag created: flag_id=%s', flag_id)
    return created({'message': 'Flag created', 'flag_id': flag_id})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
