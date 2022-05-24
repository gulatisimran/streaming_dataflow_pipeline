import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub

from utils.constants import TIMESTAMP_FORMAT
import datetime
import typing


def custom_from_proto_str(proto_msg):
    # Input type: (bytes) -> PubsubMessage
    """Construct from serialized form of ``PubsubMessage``.

    Args:
    proto_msg: String containing a serialized protobuf of type
    https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage

    Returns:
    A class object.
    """
    from google.cloud import pubsub
    msg = pubsub.types.pubsub_pb2.PubsubMessage()
    msg.ParseFromString(proto_msg)
    # Convert ScalarMapContainer to dict.
    attributes = dict((key, msg.attributes[key]) for key in msg.attributes)
    # msg.publish_time is Timestamp object with two attributes- 1. seconds 2. nanos
    # converting into seconds (float)
    publish_time_seconds = int(msg.publish_time.seconds) + \
        float(msg.publish_time.nanos / 10**9)
    published_timestamp = datetime.datetime.utcfromtimestamp(
        publish_time_seconds).strftime(TIMESTAMP_FORMAT)
    # Setting the subscribed timestamp as current timestamp
    subscribed_timestamp = datetime.datetime.utcnow().strftime(TIMESTAMP_FORMAT)
    # Setting the size of pubusub message in kilobytes
    size = len(msg.data)
    return {
        "payload": msg.data,
        "attributes": attributes,
        "published_message_id": msg.message_id,
        "published_timestamp": published_timestamp,
        "subscribed_timestamp": subscribed_timestamp,
        "size": size
    }


class ReadFromPubSub(ReadFromPubSub):
    '''
    This class is inherited from ReadFromPubSub from apache_beam library
    '''

    def __init__(self, subscription=None, with_attributes=False):
        super().__init__(subscription=subscription, with_attributes=with_attributes)

    def expand(self, pvalue):
        '''
        The expand method overrides the method in ReadFromPubSub class
        '''
        pcoll = pvalue.pipeline | beam.io.iobase.Read(self._source)
        pcoll.element_type = bytes
        if self.with_attributes:
            # Using custom_from_proto_str method to fetch the message from pubsub
            pcoll = pcoll | beam.Map(custom_from_proto_str)
            # change the data type to dictionary
            pcoll.element_type = typing.Dict
        return pcoll
