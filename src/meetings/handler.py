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
    path = event.get('path', '')
    path_params = event.get('pathParameters') or {}
    meeting_id = path_params.get('id')
    logger.info('Request: %s /meetings%s', method, f'/{meeting_id}' if meeting_id else '')

    if meeting_id and path.endswith('/attendance'):
        if method == 'POST':
            return _record_attendance(meeting_id, event)
    elif meeting_id:
        if method == 'GET':
            return _get(meeting_id)
        if method == 'PUT':
            return _update(meeting_id, event)
        if method == 'DELETE':
            return _deactivate(meeting_id)
    else:
        if method == 'GET':
            return _list()
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list():
    try:
        meetings_resp = table.scan(FilterExpression=Attr('entity_type').eq('MEETING'))
        attendance_resp = table.scan(FilterExpression=Attr('entity_type').eq('ATTENDANCE'))
        owners_resp = table.scan(FilterExpression=Attr('entity_type').eq('OWNER'))
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    owners_by_id = {
        o['owner_id']: {'name': o['name'], 'id_number': o['id_number']}
        for o in owners_resp.get('Items', [])
    }

    attendance_by_meeting = {}
    for record in attendance_resp.get('Items', []):
        attendance_by_meeting.setdefault(record['meeting_id'], set()).add(record['attendee_id'])

    meetings = meetings_resp.get('Items', [])
    for meeting in meetings:
        mid = meeting['meeting_id']
        attendee_ids = attendance_by_meeting.get(mid, set())
        absentee_ids = owners_by_id.keys() - attendee_ids
        meeting['attendees'] = [owners_by_id[i] for i in attendee_ids if i in owners_by_id]
        meeting['absentees'] = [owners_by_id[i] for i in absentee_ids]

    return ok({'meetings': meetings, 'count': len(meetings)})


def _get(meeting_id):
    try:
        resp = table.get_item(Key={'PK': f'MEETING#{meeting_id}', 'SK': 'DETAILS'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    meeting = resp.get('Item')
    if not meeting:
        logger.warning('Meeting not found: meeting_id=%s', meeting_id)
        return not_found(f'Meeting {meeting_id} not found')

    return ok({'meeting': meeting})


def _create(event):
    body = _parse_body(event)

    for field in ('title', 'date', 'start_time', 'location', 'agenda'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    meeting_id = str(uuid.uuid4())
    logger.info('Creating meeting: meeting_id=%s', meeting_id)

    try:
        table.put_item(Item={
            'PK': f'MEETING#{meeting_id}',
            'SK': 'DETAILS',
            'entity_type': 'MEETING',
            'meeting_id': meeting_id,
            'title': body['title'],
            'date': body['date'],
            'start_time': body['start_time'],
            'location': body['location'],
            'agenda': body['agenda'],
            'meeting_status': 'active',
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Meeting created: meeting_id=%s', meeting_id)
    return created({'message': 'Meeting created', 'meeting_id': meeting_id})


def _update(meeting_id, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    logger.info('Updating meeting: meeting_id=%s', meeting_id)

    try:
        table.update_item(
            Key={'PK': f'MEETING#{meeting_id}', 'SK': 'DETAILS'},
            UpdateExpression='SET title = :t, #d = :d, location = :l, updated_at = :ts',
            ExpressionAttributeNames={'#d': 'date'},
            ExpressionAttributeValues={
                ':t': body.get('title'),
                ':d': body.get('date'),
                ':l': body.get('location'),
                ':ts': _now(),
            },
        )
    except ClientError as e:
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Meeting updated: meeting_id=%s', meeting_id)
    return ok({'message': 'Meeting updated', 'meeting_id': meeting_id})


def _deactivate(meeting_id):
    logger.info('Deactivating meeting: meeting_id=%s', meeting_id)
    try:
        resp = table.update_item(
            Key={'PK': f'MEETING#{meeting_id}', 'SK': 'DETAILS'},
            UpdateExpression='SET meeting_status = :s, updated_at = :ts',
            ConditionExpression=Attr('meeting_id').exists(),
            ExpressionAttributeValues={':s': 'inactive', ':ts': _now()},
            ReturnValues='UPDATED_NEW',
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return not_found(f'Meeting {meeting_id} not found')
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Meeting deactivated: meeting_id=%s', meeting_id)
    return ok({'message': 'Meeting deactivated', 'meeting_id': meeting_id})


def _record_attendance(meeting_id, event):
    body = _parse_body(event)
    attendee_ids = body.get('attendee_ids', [])

    if not attendee_ids or not isinstance(attendee_ids, list):
        return bad_request('attendee_ids must be a non-empty list')

    logger.info('Recording attendance: meeting_id=%s count=%d', meeting_id, len(attendee_ids))

    try:
        with table.batch_writer() as batch:
            for attendee_id in attendee_ids:
                batch.put_item(Item={
                    'PK': f'MEETING#{meeting_id}',
                    'SK': f'ATTENDANCE#{attendee_id}',
                    'entity_type': 'ATTENDANCE',
                    'meeting_id': meeting_id,
                    'attendee_id': attendee_id,
                    'recorded_at': _now(),
                })
    except ClientError as e:
        logger.error('DynamoDB batch_writer failed: %s', e.response['Error'])
        return server_error()

    return created({
        'message': 'Attendance recorded',
        'meeting_id': meeting_id,
        'attendee_count': len(attendee_ids),
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
