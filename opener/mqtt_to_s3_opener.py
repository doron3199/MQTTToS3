import boto3
import json
import sys
S3_ENDPOINT_URL = ""

class Mqtt_to_s3_bucket_opener:
    def __init__(self, s3_resource, bucket_name, start, end):
        self.bucket_name = bucket_name
        self.start = start
        self.end = end
        
        # Get the list of objects in the bucket in the time raznge
        # get all objects names in the bucket
        bucket = s3_resource.Bucket(bucket_name)
        self.objects_keys = [int(obj.key) for obj in bucket.objects.all() if obj.key.isnumeric() and int(obj.key) >= start and int(obj.key) <= end]
        self.objects = [Mqtt_to_s3_object_opener(self.bucket_name, str(key)) for key in self.objects_keys]

    def get_messages_from_object(self, obj):
        # Get all messages from the object
        messages = []
        for message in obj.get_messages():
            messages.append(message)
        return messages

    def get_all_messages(self):
        # Get all messages from the objects
        messages = []
        for obj in self.objects:
            messages += self.get_messages_from_object(obj)
        return messages

    def get_messages_by_time_range(self, start, end):
        # Get all messages from the objects in the time range
        messages = []
        for obj in self.objects:
            messages += obj.get_messages_by_time_range(start, end)
        return messages
    
    def get_messages_by_topic(self, topic):
        # Get all messages from the objects in the time range
        messages = []
        for obj in self.objects:
            messages += obj.get_messages_by_topic(topic)
        return messages
    
    def get_topic_list(self):
        # Get all messages from the objects in the time range
        topics = []
        for obj in self.objects:
            topics += obj.get_topic_list()
        return list(set(topics))


class Mqtt_to_s3_object_opener:
    def __init__(self, s3_client, bucket, key):
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key

        head = self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
        self.header_size = int(head['Metadata']['mqtt-header-size'])
        self.header = self.s3_client.get_object(Bucket=self.bucket, Key=self.key, Range=f'bytes=0-{self.header_size-1}')
        self.header = json.loads(self.header['Body'].read())
        self.tiles = self.header['tiles']
        self.add_size_to_tiles()
        self.length = len(self.tiles)
    
    def add_size_to_tiles(self):
        for index in range(len(self.tiles)):
            if index == len(self.tiles) - 1:
                self.tiles[index]['Size'] = len(self.get_part_of_body(self.tiles[index]['Offset'], ''))
            else:
                self.tiles[index]['Size'] = self.tiles[index + 1]['Offset'] - self.tiles[index]['Offset']

    def get_messages(self):
        return self.get_messages_by_range(0, self.length - 1)

    def get_object_body(self):
        return s3client.get_object(Bucket=self.bucket, Key=self.key)['Body'].read()
    
    def get_part_of_body(self, start, end):
        start = 0 if start == '' else start
        end = 0 if end == '' else end
        return self.s3_client.get_object(Bucket=self.bucket, Key=self.key, Range=f'bytes={self.header_size + start}-{self.header_size + end}')['Body'].read()

    def get_message_by_tile(self, tile):
        return self.get_part_of_body(tile['Offset'], tile['Offset'] + tile['Size'] - 1)

    def get_message_by_index(self, index):
        return self.get_part_of_body(self.tiles[index]['Offset'], self.tiles[index]['Offset'] + self.tiles[index]['Size'] - 1)

    def get_messages_by_range(self, start, end):
        start_offset = self.tiles[start]['Offset']
        if end >= self.length:
            end_offset = self.tiles[self.length - 1]['Offset'] + self.tiles[self.length - 1]['Size']
        else:
            end_offset = self.tiles[end]['Offset'] + self.tiles[end]['Size'] - 1
        
        range_bytes = self.get_part_of_body(start_offset, end_offset)

        # isolate the messages fron the byte stream
        messages = []
        for index in range(start, end):
            messages.append(range_bytes[self.tiles[index]['Offset']:self.tiles[index]['Offset'] + self.tiles[index]['Size'] ])
        
        return messages

    def get_topic_list(self):
        return list(set([tile['Topic'] for tile in self.tiles]))
    
    def get_time_range(self):
        return self.tiles[0]['UnixTime'], self.tiles[-1]['UnixTime']
    
    def get_messages_by_tiles(self, tiles):
        messages = []
        for tile in tiles:
            messages.append(self.get_message_by_tile(tile))
        return messages

    def get_messages_by_topic(self, topic):
        tiles = [tile for tile in self.tiles if tile['Topic'] == topic]
        return self.get_messages_by_tiles(tiles)

    def get_messages_by_time_range(self, start, end):
        tiles = [tile for tile in self.tiles if tile['UnixTime'] >= start and tile['UnixTime'] <= end]
        return self.get_messages_by_tiles(tiles)

    def get_next_tile(self):
        if self.index == self.length:
            return None
        tile = self.tiles[self.index]
        self.index += 1
        return tile, self.get_part_of_body(self.bucket, self.key, tile['Offset'] + self.header_size, tile['Offset'] + self.header_size + tile['Size'] - 1)

if __name__ == '__main__':
    if len(sys.argv != 5) or len(sys.argv[1]) != 6:
        print('Usage: python3 mqtt_to_s3_object_opener.py <region> <endpoint> <bucket_name> <start_time> <end_time>')
        print('       or')
        print('       python3 mqtt_to_s3_object_opener.py <region> <endpoint> <bucket_name> <key>')
        exit()

    # the credentials will look by levels in the provider chain
	# for convince here are the names of the environment variables
	# AWS_ACCESS_KEY_ID
	# AWS_SECRET_ACCESS_KEY
	# AWS_SESSION_TOKEN
    
    if len(sys.argv) == 4:
        end_point, region, bucket, start_epoch, end_epoch = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
        s3_resource = boto3.client('s3', endpoint_url = end_point, region_name = region)
        mqtt_to_s3_bucket = Mqtt_to_s3_bucket_opener(s3_resource, bucket,start_epoch, end_epoch)

        # examples
        # mqtt_to_s3_bucket.get_messages_by_topic('test')
        # mqtt_to_s3_bucket.get_messages_by_time_range(1589749600, 1589750000)
        # Mqtt_to_s3_bucket_opener.get_topic_list()
        # Mqtt_to_s3_bucket_opener.get_all_messages()
    else:
        end_point, region, bucket, key = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
        s3_resource = boto3.client('s3', endpoint_url = end_point, region_name = region)
        mqtt_package = Mqtt_to_s3_object_opener(s3_resource, bucket, key)

        # examples
        # mqtt_package.get_messages()
        # mqtt_package.get_topic_list()
        # mqtt_package.get_time_range()
        # mqtt_package.get_messages_by_topic('test')
        # mqtt_package.get_messages_by_time_range(1589749600, 1589750000)
