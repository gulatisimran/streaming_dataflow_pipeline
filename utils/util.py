import datetime
import logging

from utils.constants import *


def is_valid_timestamp(value):
    """
    This method returns True if a string is valid timestamp
    :param value : str - any value
    >>> is_valid_timestamp('2020-07-10T16:19:11.395539Z')
    True
    """
    try:
        datetime_value = datetime.datetime.strptime(value, TIMESTAMP_FORMAT)
        return True
    except Exception as e:
        logging.debug(
            f"Exception in timestamp conversion for value {value} - {e}")
        return False


def is_not_future_or_past_timestamp(value, past_threshold, post_threshold):
    try:
        datetime_value = datetime.datetime.strptime(value, TIMESTAMP_FORMAT)
        current_timestamp = datetime.datetime.utcnow()
        delta = datetime_value - current_timestamp
        hours = delta.days * 24 + \
            (delta.seconds / 3600) + (delta.microseconds / 360000000)
        if hours > 1 * int(post_threshold):
            return False, f"is a future date {FUTURE_TIMESTAMP_ERROR} hrs"
        elif hours < -1 * int(past_threshold):
            return False, f"is older than {past_threshold} hrs"
        return True, ""
    except Exception as e:
        logging.debug(
            f"Exception in timestamp conversion- for value {value}:- {e}")
        return False, TIMESTAMP_CONVERSION_ERROR


def is_empty(value):
    """
    This method returns True if the value is Null or An Empty String
    >>> is_empty('')
    True
    """
    if value is None or str(value).strip() == '' or str(value).lower() == 'none':
        return True
    return False


def is_valid_integer(value):
    """
    >>> is_valid_integer(25)
    True
    >>> is_valid_integer(25.5)
    False
    """
    try:
        # this method int(value) throws TypeError if value is not an integer, handled in except
        if int(value) >= 0 and '.' not in str(value):
            return True
        return False
    except:
        return False


def is_valid_float(value):
    """
    this method float(value) throws ValueError if value is not a float, handled in except
    >>> is_valid_float(25.6)
    True
    """
    try:
        if float(value) >= 0:
            return True
        return False
    except:
        return False


def populate_partition_key(element, timestamp_col, partition_col, past_data_threshold, post_data_threshold):
    if element.setdefault(timestamp_col) is None or \
            not (is_valid_timestamp(element.get(timestamp_col))) or \
            not (is_not_future_or_past_timestamp(element.get(timestamp_col), past_data_threshold, post_data_threshold)[0]):
        element[partition_col] = str(datetime.datetime.utcnow().date())
    else:
        element[partition_col] = element.get(timestamp_col).split('T')[0]
    return element



def populate_reject_reason(element, reject_reason, bq_schema, ignore_cols, past_data_threshold, post_data_threshold):
    for column in bq_schema:
        reject = False
        column_name = column.get('name')
        column_value = str(element.get(column_name)).strip()

        if column_name in ignore_cols:
            continue
        if column.get('mode') == COLUMN_MODE_NULLABLE and is_empty(column_value):
            element[column_name] = None

        # To check condition if a column is required in bq and is also empty then reject = True
        elif column.get('mode') == COLUMN_MODE_REQUIRED and is_empty(column_value):
            reject = True
            description = f"{column_name} {EMPTY_ERROR}"

        # To check condition if bigquery column type is timestamp column or not and also it is not empty
        # and it will validate weather timestamp column is a future or past timestamp or not.
        elif column.get('type') == COLUMN_TYPE_TIMESTAMP and not is_empty(column_value):
            ts_valid = is_not_future_or_past_timestamp(
                column_value, past_data_threshold, post_data_threshold)
            if not ts_valid[0]:
                reject = True
                description = f"{column_name} {ts_valid[1]}"

        # To check condition if bigquery column type is integer
        elif column.get('type') == COLUMN_TYPE_INTEGER:
            if not is_valid_integer(column_value):
                reject = True
                description = f"{column_name} {INTEGER_ERROR}"

        elif column.get('type') == COLUMN_TYPE_FLOAT:
            if not is_valid_float(column_value):
                reject = True
                description = f"{column_name} {FLOAT_ERROR}"

        # To check condition if bigquery column type is boolean
        elif column.get('type') == COLUMN_TYPE_BOOLEAN:
            boolean_val = str(column_value).lower()
            if boolean_val not in BOOLEAN_VALUES:
                reject = True
                description = f"{column_name} {BOOLEAN_ERROR}"
            if boolean_val in BOOLEAN_VALUES:
                element[column_name] = boolean_val

        # To check condition if bigquery column type is TIMESTAMP
        elif column.get('type') == COLUMN_TYPE_TIMESTAMP:
            if not is_valid_timestamp(column_value):
                reject = True
                description = f"{column_name} {TIMESTAMP_CONVERSION_ERROR}"
        else:
            continue

        # check if reject is True, if yes then append the reason dict to list
        if reject:
            reject_reason.append({'source_column': column_name,
                                  'source_value': column_value,
                                  'description': description
                                  })

    return element, reject_reason
