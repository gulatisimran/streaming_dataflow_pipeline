VOICE_BQ_ROW = {
    "call_id": None,
    "status": None,
    "call_start": None,
    "caller_id": None,
    "call_hangup": None,
    "disconnect_reason": None,
    "call_duration": None
}

VOICE_BQ_SCHEMA = [
    {
        "mode": "NULLABLE",
        "name": "call_id",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "status",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "call_start",
        "type": "TIMESTAMP"
    },
    {
        "mode": "NULLABLE",
        "name": "caller_id",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "call_hangup",
        "type": "TIMESTAMP"
    },
    {
        "mode": "NULLABLE",
        "name": "disconnect_reason",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "call_duration",
        "type": "FLOAT"
    },
    {
        "mode": "NULLABLE",
        "name": "call_start_date",
        "type": "DATE"
    },
    {
        "mode": "REQUIRED",
        "name": "load_timestamp",
        "type": "TIMESTAMP"
    },
    {
        "mode": "REQUIRED",
        "name": "published_timestamp",
        "type": "TIMESTAMP"
    },
    {
        "mode": "REQUIRED",
        "name": "subscribed_timestamp",
        "type": "TIMESTAMP"
    },
    {
        "mode": "REQUIRED",
        "name": "published_message_id",
        "type": "STRING"
    }
]
