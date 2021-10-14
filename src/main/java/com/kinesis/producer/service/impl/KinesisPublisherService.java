package com.kinesis.producer.service.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.kinesis.producer.service.ProducerService;

@Service
public class KinesisPublisherService implements ProducerService {

	private static final Logger logger = LoggerFactory.getLogger(KinesisPublisherService.class);

	@Value("${aws.stream_name}")
	private String streamName;

	@Value("${aws.region}")
	private String awsRegion;

	@Value("${aws.access_key}")
	private String awsAccessKey;

	@Value("${aws.secret_key}")
	private String awsSecretKey;

	private AmazonKinesis kinesisProducer = null;

	// The number of records that have finished (either successfully put, or failed)
	final AtomicLong completed = new AtomicLong(0);

	private static final String TIMESTAMP_AS_PARTITION_KEY = Long.toString(System.currentTimeMillis());

	public KinesisPublisherService() {
		this.kinesisProducer = getKinesisProducer();
	}

	private AmazonKinesis getKinesisProducer() {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIA6Q477HDAF5WOWXPL","9FczAdu7owZCLycQJUwFbERWDrjwYzReGfFuxywr");
		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
		clientBuilder.setRegion("ap-south-1");
		clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCreds));
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxConnections(1);
		config.setRequestTimeout(6000); // 6 seconds
		clientBuilder.setClientConfiguration(config);
		return clientBuilder.build();
	}

	@Override
	public void putDataIntoKinesis(String payload) throws Exception {
		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
		logger.info("Printing the Payload:-"+payload);
		putRecordsRequest.setStreamName(streamName);
		logger.info("Converting the Payload String to Bytes");
		try {
			String i = UUID.randomUUID().toString();
			PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
			putRecordsRequestEntry.setData(ByteBuffer.wrap(payload.getBytes("UTF-8")));
			putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", 1));
			putRecordsRequestEntryList.add(putRecordsRequestEntry);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		logger.info("Sending the Data to Kinesis Data Stream");
		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		PutRecordsResult putRecordsResult = kinesisProducer.putRecords(putRecordsRequest);
		logger.info(putRecordsResult.getFailedRecordCount().toString());
	}

}