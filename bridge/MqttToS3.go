package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

var SizeOfUint32InBytes = 4

type Conf struct {
	Broker        string              `yaml:"broker"`
	Buckets       map[string][]string `yaml:"buckets"`
	S3Region      string              `yaml:"s3Region"`
	S3Endpoint    string              `yaml:"s3Endpoint"`
	StopCondition string              `yaml:"stopCondition"`
	MaxSize       int                 `yaml:"maxSize"`
}

type HeaderTile struct {
	Offset   int
	UnixTime int64
	Topic    string
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Message %s received on topic %s\n", msg.Payload(), msg.Topic())
}

var (
	numberOfMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "total_aggregated_messages",
		Help: "The total number of aggregated messages",
	})
	numberOfObjects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "total_objects",
		Help: "The total number of objects uploaded",
	})
)

func messageHandlerCreator(c chan mqtt.Message) func(mqtt.Client, mqtt.Message) {
	// gets a channel, and return a function that send messages to this channel
	return func(client mqtt.Client, msg mqtt.Message) {
		c <- msg
	}
}

func uploadToS3(bucket string, time int, payload []byte, headerSize int, uploader *s3manager.Uploader) {

	// actual code to upload to s3
	numberOfObjects.Inc()
	m := make(map[string]*string)
	str := strconv.Itoa(headerSize)
	m["mqtt-header-size"] = &str
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(strconv.Itoa(time)),
		Body:     bytes.NewReader(payload),
		Metadata: m,
	})
	if err != nil {
		fmt.Printf("failed to upload file, %v\n", err)
		return
	}
	fmt.Printf("Message at time %d was uploaded to bucket %s\n ID = %s, location = %s",
		time, bucket, result.UploadID, result.Location)
}

const (
	timeInSeconds int = 0
	sizeInBytes       = 1
	sizeInLength      = 2
)

func mqttPayloadArrToS3Payload(payloadArr [][]byte, unixTimeArray []int64, topicArray []string) ([]byte, int) {
	tiles := make([]HeaderTile, 0)
	var body []byte
	offset := 0

	for index, payload := range payloadArr {
		tile := HeaderTile{
			Offset:   offset,
			UnixTime: unixTimeArray[index],
			Topic:    topicArray[index]}
		tiles = append(tiles, tile)
		body = append(body, payload...)
		offset += len(payload)
	}
	header := map[string][]HeaderTile{"tiles": tiles}
	jsonHeader, _ := json.Marshal(header)
	headerSize := len(jsonHeader)
	finalPayload := make([]byte, len(jsonHeader)+len(body))
	copy(finalPayload[:headerSize], jsonHeader)
	copy(finalPayload[headerSize:], body)
	fmt.Printf("header size is %v \nbody size is %v\nfile size is %v\n", headerSize, len(body), len(finalPayload))
	return finalPayload, headerSize
}

func receiver(bucket string, channel chan mqtt.Message, uploader *s3manager.Uploader, stopCondition int,
	maxSize int) {
	var payloadArr [][]byte
	size := 0
	var unixTimeArr []int64
	var topicArr []string
	startTime := time.Now()

	for msg := range channel {
		numberOfMessages.Inc()
		fmt.Printf("Message %s received on topic %s and will be uploaded to bucket %s\n",
			msg.Payload(), msg.Topic(), bucket)
		unixTimeArr = append(unixTimeArr, time.Now().Unix())
		payloadArr = append(payloadArr, msg.Payload())
		topicArr = append(topicArr, msg.Topic())
		switch stopCondition {
		case sizeInLength:
			size += 1
			if size >= maxSize {
				size = 0
				payload, headerSize := mqttPayloadArrToS3Payload(payloadArr, unixTimeArr, topicArr)
				uploadToS3(bucket, int(time.Now().Unix()), payload, headerSize, uploader)
				// clear arrays
				payloadArr, unixTimeArr, topicArr = payloadArr[:0], unixTimeArr[:0], topicArr[:0]
			}
		case sizeInBytes:
			size += len(msg.Payload()) + len(msg.Topic()) + SizeOfUint32InBytes
			// notice: can be also len(payload_arr) >= maxSize
			if size >= maxSize {
				size = 0
				payload, headerSize := mqttPayloadArrToS3Payload(payloadArr, unixTimeArr, topicArr)
				uploadToS3(bucket, int(time.Now().Unix()), payload, headerSize, uploader)
				// clear arrays
				payloadArr, unixTimeArr, topicArr = payloadArr[:0], unixTimeArr[:0], topicArr[:0]
			}
		case timeInSeconds:
			timeInSecond := int(startTime.Sub(time.Now()) / time.Second)
			if timeInSecond >= maxSize {
				startTime = time.Now()
				payload, headerSize := mqttPayloadArrToS3Payload(payloadArr, unixTimeArr, topicArr)
				uploadToS3(bucket, int(time.Now().Unix()), payload, headerSize, uploader)
				// clear arrays
				payloadArr, unixTimeArr, topicArr = payloadArr[:0], unixTimeArr[:0], topicArr[:0]
			}
		}

	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection Lost: %s\n", err.Error())
}

func main() {
	var yamlPath string
	if len(os.Args) == 2 {
		yamlPath = os.Args[1]
	} else {
		fmt.Println("Using default yaml path")
		yamlPath = "./conf.yaml"
	}

	// read config file
	yamlFile, err := ioutil.ReadFile(yamlPath)
	if err != nil {
		log.Fatalf("yamlFile.Get err   #%v ", err)
	}

	// convert it to Conf interface
	conf := Conf{}
	err = yaml.Unmarshal(yamlFile, &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Println(conf)
	var stopCondition int
	switch conf.StopCondition {
	case "bytes":
		stopCondition = sizeInBytes
	case "length":
		stopCondition = sizeInLength
	case "time":
		stopCondition = timeInSeconds
	}

	// mqtt utils
	options := mqtt.NewClientOptions()
	options.AddBroker(conf.Broker)
	options.SetClientID("go_mqtt_to_s3")
	options.SetDefaultPublishHandler(messagePubHandler)
	options.OnConnect = connectHandler
	options.OnConnectionLost = connectionLostHandler
	client := mqtt.NewClient(options)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// s3 utils
	forcePathStyle := true
	disableSSL := true
	var config aws.Config
	if conf.S3Endpoint != "" {
		config = aws.Config{
			Region:           aws.String(conf.S3Region),
			Endpoint:         aws.String(conf.S3Endpoint),
			S3ForcePathStyle: &forcePathStyle,
			DisableSSL:       &disableSSL,
		}
	} else {
		config = aws.Config{
			Region: aws.String(conf.S3Region)}
	}
	sess, err := session.NewSession(&config)
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	// subscribe the buckets to their channels
	for bucket, topics := range conf.Buckets {
		// for each bucket create a channel
		tmpChannel := make(chan mqtt.Message)
		// the function that will receive all the messages that go to the bucket
		go receiver(bucket, tmpChannel, uploader, stopCondition, conf.MaxSize)
		// the function that is called after mqtt received a message in a channel
		messageHandler := messageHandlerCreator(tmpChannel)
		// subscribe to the channels and set the message handler ass the callbcak
		for _, topic := range topics {
			token = client.Subscribe(topic, 1, messageHandler)
			token.Wait()
			fmt.Printf("Bucket %s is subscribed to topic %s\n", bucket, topic)
		}
	}

	http.Handle("/metrics", promhttp.Handler())
	errPrem := http.ListenAndServe(":2112", nil)
	if errPrem != nil {
		fmt.Println("Prometheus failed")
		client.Disconnect(100)
		return
	}
}
