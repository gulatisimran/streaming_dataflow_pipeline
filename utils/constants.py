TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
FILE_NAME_FORMAT = "%H:%M:%S.jsonl"
FOLDER_NAME_FORMAT = "load_date=%Y-%m-%d"

TIMESTAMP_CONVERSION_ERROR = "was not able to convert to timestamp"
FUTURE_TIMESTAMP_ERROR = "is a future timestamp"
INTEGER_ERROR = "is not an integer"
FLOAT_ERROR = "is not a float"
BOOLEAN_ERROR = "is not a boolean"
EMPTY_ERROR = "is empty"
JSON_ERROR = "Json not parsed"

COLUMN_TYPE_TIMESTAMP = "TIMESTAMP"
COLUMN_TYPE_INTEGER = "INTEGER"
COLUMN_TYPE_FLOAT = "FLOAT"
COLUMN_TYPE_BOOLEAN = "BOOLEAN"

COLUMN_MODE_NULLABLE = "NULLABLE"
COLUMN_MODE_REQUIRED = "REQUIRED"
COLUMN_MODE_REPEATED = "REPEATED"
COLUMN_MODE_RECORD = "RECORD"

BOOLEAN_VALUES = ["true", "false"]

# Constants used in testing pipelines
LIST_OF_TEST_PIPELINE_NAMES = [
    'voice_pipeline', 'sms_mo_pipeline', 'sms_mt_pipeline', 'delivery_receipt_pipeline']
LIST_OF_TEST_MODULE_NAMES = ['create_row', 'validate_row']

CXL_TYPES = ["exit", "transfer", "datarequest", "say", "prompt", "component"]
