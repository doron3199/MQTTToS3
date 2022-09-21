# MQTT to S3 bridge

## why?
S3 is an object storage protocol that allow us to store objects.  
In some situations we would need to store MQTT messages for  
various reasons, for example, onlineanalytical processing (OLAP).  
In the case of storing MQTT messages with the S3 protocol,  
there ara tradeoffs that could be mitigated with aggregation of   
the MQTT messages and storing them in batches.

This is exactly what the MQTT to S3 project is set to do,  
to increase the efficiency of using MQTT with S3 by mitigating the  
aforementioned tradeoffs: improving the network efficiency   
(less “expensive” encapsulations of small amount of data in the messages).

## how to upload an object?
### MQTT
Have a mqtt broker, write in the conf file the channels that you 
want to collect messages from.
we recommend to use https://www.eclipse.org/paho/index.php?page=downloads.php
client

You can check the that the broker works with this code, pase it before line 250
The code sends each topic in each bucket {num} messages.

	num := 5
	for _, topics := range conf.Buckets {
		for _, topic := range topics {
			for i := 0; i < num; i++ {
				text := fmt.Sprintf("hey %d", i)
				token = client.Publish(topic, 0, false, text)
				token.Wait()
				time.Sleep(time.Second)
			}
		}
	}

	// before this line
	http.Handle("/metrics", promhttp.Handler())

### S3 bucket
The program	 was checked on AWS S3 and CEPH S3 and supposed to work with S3 protocols from any service. the only thing yu need to do is
to create a bucket.
### dependencies
clone the project, go in to the bridge folder and run "go mod tidy"  
to download all the dependencies
### configuration
here is an example for the yaml file:



    broker: tcp://test.mosquitto.org:1883  
	stopCondition: 'length'  
	maxSize: 500  
	s3Region: 'us-east-2'  
	s3Endpoint: 'example/endpoint'  
	buckets:  
		example-bucket-name1:  
	       - example/topic1
	       - example/toppic2
	    example-bucket-name2:  
	       - example/topic3
	       - example/toppic4



So what are the fields?  
The mqtt broker url:  
broker: tcp://test.mosquitto.org:1883

This is how to control how to divide messages to object
* length - upload object after it have {maxSize} messages
* size - upload object after it have {maxSize} bytes
* time - upload object after {maxSize} seconds passed  
  stopCondition: 'length'  
  maxSize: int

S3 region and end point, for credentials check this link  
https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#:~:text=profile%20you%20specify.-,Environment%20Variables,-By%20default%2C%20the  
for convince here are the names of the environment variables  
AWS_ACCESS_KEY_ID  
AWS_SECRET_ACCESS_KEY  
AWS_SESSION_TOKEN  
s3Region: 'us-east-2'  
s3Endpoint: 'example/endpoint'

The buckets field gets a string -> array[string] map
For each bucket name specify the names of the topics you want in it

	buckets:  
	    example-bucket-name1:  
	       - example/topic1
	       - example/toppic2
	    example-bucket-name2:  
	       - example/topic3
	       - example/toppic4

## how to download an object?
for convenience there is a python script that gives
an example of how to open the object. it's very straight
forward easy to understand even if you don't know python. you can run it with
one of those commands, but its recommended
to play with it in the console while debugging.
notice that in the script you can choose to work with
a bucket or with a specific object. 
python3 mqtt_to_s3_object_opener.py <region> <endpoint> <bucket_name> <key>
or 
python3 mqtt_to_s3_object_opener.py <region> <endpoint> <bucket_name> <start_time> <end_time>

the S3 object have 3 components
* metadata, inside the object head - mqtt-header-size
* header, inside the object itself
* body, inside the object itself

the program was build so you won't need to download
the whole object, only the parts that you need.
so first get the mqtt-header-size, and get 
byte 0 to mqtt-header-size from the object body, 
this is a json string. convert it to a dictionary in your
preferable language. the json contain one field - 
tiles that it contains an array. each element in 
the array have unix time, topic name, and start offset
you can filter the messages by those topic
to get message N - get bytes from object from 
offset N to offset N + 1 (or to end of file for the last one)
