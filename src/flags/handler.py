import json
import uuid

from common.response import bad_request, created, method_not_allowed, ok

# TODO: Uncomment when DynamoDB is wired up
# import os
# import boto3
# from boto3.dynamodb.conditions import Key, Attr
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['TABLE_NAME'])

VALID_ENTITY_TYPES = ('owner', 'driver', 'vehicle')
VALID_SEVERITIES = ('low', 'medium', 'high')


def lambda_handler(event, context):
    method = event.get('httpMethod', '')

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

    # TODO: Query DynamoDB with optional filters
    # filter_exprs = []
    # if entity_id:
    #     filter_exprs.append(Attr('entity_id').eq(entity_id))
    # if entity_type:
    #     filter_exprs.append(Attr('entity_type').eq(entity_type))
    # if severity:
    #     filter_exprs.append(Attr('severity').eq(severity))
    # ...
    flags = []
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

    # TODO: Store in DynamoDB
    # table.put_item(Item={
    #     'pk': f'FLAG#{flag_id}',
    #     'sk': 'DETAILS',
    #     'entity_type': 'FLAG',
    #     'flag_id': flag_id,
    #     'entity_id': body['entity_id'],
    #     'flagged_entity_type': body['entity_type'],
    #     'reason': body['reason'],
    #     'severity': severity,
    #     'resolved': False,
    #     'created_at': _now(),
    # })

    return created({'message': 'Flag created', 'flag_id': flag_id})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}
