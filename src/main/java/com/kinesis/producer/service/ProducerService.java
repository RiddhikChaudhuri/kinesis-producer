package com.kinesis.producer.service;

public interface ProducerService {

	public void putDataIntoKinesis(String payload) throws Exception;


}