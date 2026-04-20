import logging

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from common.response import bad_request, method_not_allowed, ok, server_error

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('zimeleni-transport')


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    logger.info('Request: %s /search', method)

    if method != 'GET':
        return method_not_allowed()

    return _search(event)


# ── Handlers ──────────────────────────────────────────────────────────────────

def _search(event):
    query_params = event.get('queryStringParameters') or {}
    reg_number = query_params.get('reg_number', '').strip()
    id_number = query_params.get('id_number', '').strip()

    if not reg_number and not id_number:
        return bad_request('Provide reg_number or id_number as a query parameter')

    if reg_number:
        return _by_reg_number(reg_number)
    return _by_id_number(id_number)


def _by_reg_number(reg_number):
    logger.info('Search by reg_number=%s', reg_number)
    try:
        resp = table.query(
            IndexName='reg_number-index',
            KeyConditionExpression=Key('reg_number').eq(reg_number.upper()),
        )
    except ClientError as e:
        logger.error('DynamoDB query failed: %s', e.response['Error'])
        return server_error()

    results = resp.get('Items', [])
    return ok({'query': {'reg_number': reg_number}, 'results': results, 'count': len(results)})


def _by_id_number(id_number):
    logger.info('Search by id_number=%s', id_number)
    try:
        resp = table.query(
            IndexName='id_number-index',
            KeyConditionExpression=Key('id_number').eq(id_number),
        )
    except ClientError as e:
        logger.error('DynamoDB query failed: %s', e.response['Error'])
        return server_error()

    results = resp.get('Items', [])
    return ok({'query': {'id_number': id_number}, 'results': results, 'count': len(results)})
