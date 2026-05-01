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
    elif meeting_id and path.endswith('/minutes'):
        if method == 'POST':
            return _save_minutes(meeting_id, event)
        if method == 'GET':
            return _get_minutes(meeting_id)
    elif meeting_id and path.endswith('/lock'):
        if method == 'PUT':
            return _lock(meeting_id)
    elif meeting_id:
        if method == 'GET':
            return _get(meeting_id)
        if method == 'PUT':
            return _update(meeting_id, event)
        if method == 'DELETE':
            return _deactivate(meeting_id)
    else:
        if method == 'GET':
            return _list(event)
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list(event):
    query_params = event.get('queryStringParameters') or {}
    filter_locked = query_params.get('locked', '').lower() == 'true'

    try:
        meetings_resp = table.scan(FilterExpression=Attr('entity_type').eq('MEETING'))
        attendance_resp = table.scan(FilterExpression=Attr('entity_type').eq('ATTENDANCE'))
        owners_resp = table.scan(FilterExpression=Attr('entity_type').eq('OWNER'))
        minutes_resp = table.scan(FilterExpression=Attr('entity_type').eq('MINUTES'))
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    owners_by_id = {
        o['id_number']: {'name': o['name'], 'id_number': o['id_number']}
        for o in owners_resp.get('Items', [])
    }

    attendance_by_meeting = {}
    for record in attendance_resp.get('Items', []):
        mid = record['meeting_id']
        attendance_by_meeting.setdefault(mid, {})[record['attendee_id']] = record.get('attendee_status', 'present')

    minutes_by_meeting = {
        record['meeting_id']: record
        for record in minutes_resp.get('Items', [])
    }

    meetings = meetings_resp.get('Items', [])
    if filter_locked:
        meetings = [m for m in meetings if m.get('is_locked') is True]

    for meeting in meetings:
        mid = meeting['meeting_id']
        attendee_map = attendance_by_meeting.get(mid, {})
        absentee_ids = owners_by_id.keys() - attendee_map.keys()
        meeting['attendees'] = [
            {**owners_by_id[i], 'attendee_status': attendee_map[i]}
            for i in attendee_map if i in owners_by_id
        ]
        meeting['absentees'] = [owners_by_id[i] for i in absentee_ids]
        if mid in minutes_by_meeting:
            meeting['minutes'] = minutes_by_meeting[mid]

    return ok({'meetings': meetings, 'count': len(meetings)})


def _get(meeting_id):
    try:
        details_resp = table.get_item(Key={'PK': f'MEETING#{meeting_id}', 'SK': 'DETAILS'})
        minutes_resp = table.get_item(Key={'PK': f'MEETING#{meeting_id}', 'SK': 'MINUTES'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    meeting = details_resp.get('Item')
    if not meeting:
        logger.warning('Meeting not found: meeting_id=%s', meeting_id)
        return not_found(f'Meeting {meeting_id} not found')

    minutes = minutes_resp.get('Item')
    if minutes:
        meeting['minutes'] = minutes

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


def _lock(meeting_id):
    logger.info('Locking meeting: meeting_id=%s', meeting_id)
    try:
        table.update_item(
            Key={'PK': f'MEETING#{meeting_id}', 'SK': 'DETAILS'},
            UpdateExpression='SET is_locked = :l, locked_at = :ts',
            ConditionExpression=Attr('meeting_id').exists(),
            ExpressionAttributeValues={':l': True, ':ts': _now()},
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return not_found(f'Meeting {meeting_id} not found')
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Meeting locked: meeting_id=%s', meeting_id)
    return ok({'message': 'Meeting locked', 'meeting_id': meeting_id})


def _record_attendance(meeting_id, event):
    body = _parse_body(event)
    attendees = body.get('attendees', [])

    if not attendees or not isinstance(attendees, list):
        return bad_request('attendees must be a non-empty list')

    logger.info('Recording attendance: meeting_id=%s count=%d', meeting_id, len(attendees))

    try:
        with table.batch_writer() as batch:
            for attendee in attendees:
                id_number = attendee.get('id_number')
                if not id_number:
                    continue
                batch.put_item(Item={
                    'PK': f'MEETING#{meeting_id}',
                    'SK': f'ATTENDANCE#{id_number}',
                    'entity_type': 'ATTENDANCE',
                    'meeting_id': meeting_id,
                    'attendee_id': id_number,
                    'attendee_status': attendee.get('attendee_status', 'present'),
                    'recorded_at': _now(),
                })
    except ClientError as e:
        logger.error('DynamoDB batch_writer failed: %s', e.response['Error'])
        return server_error()

    return created({
        'message': 'Attendance recorded',
        'meeting_id': meeting_id,
        'attendee_count': len(attendees),
    })


def _save_minutes(meeting_id, event):
    body = _parse_body(event)

    content = body.get('content')
    if not content:
        return bad_request('content is required')

    try:
        existing = table.get_item(Key={'PK': f'MEETING#{meeting_id}', 'SK': 'DETAILS'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    if not existing.get('Item'):
        return not_found(f'Meeting {meeting_id} not found')

    now = _now()
    item = {
        'PK': f'MEETING#{meeting_id}',
        'SK': 'MINUTES',
        'entity_type': 'MINUTES',
        'meeting_id': meeting_id,
        'content': content,
        'updated_at': now,
    }
    if body.get('recorded_by'):
        item['recorded_by'] = body['recorded_by']

    try:
        existing_minutes = table.get_item(Key={'PK': f'MEETING#{meeting_id}', 'SK': 'MINUTES'})
        if not existing_minutes.get('Item'):
            item['created_at'] = now
        table.put_item(Item=item)
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Minutes saved: meeting_id=%s', meeting_id)
    return ok({'message': 'Minutes saved', 'meeting_id': meeting_id})


def _get_minutes(meeting_id):
    try:
        resp = table.get_item(Key={'PK': f'MEETING#{meeting_id}', 'SK': 'MINUTES'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    minutes = resp.get('Item')
    if not minutes:
        return not_found(f'No minutes found for meeting {meeting_id}')

    return ok({'minutes': minutes})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
