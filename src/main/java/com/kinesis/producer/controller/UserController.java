package com.kinesis.producer.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kinesis.producer.entity.UserProfile;
import com.kinesis.producer.service.ProducerService;

@RestController
@CrossOrigin
@RequestMapping("/user")
public class UserController {

	@Autowired
	private ProducerService producerService;

	@PostMapping("/bulk-insert")
	public ResponseEntity<?> createNewInvestor(@RequestBody UserProfile userProfileList,
			HttpServletRequest httpServletRequest) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		String data = "";
		try {
			data = mapper.writeValueAsString(userProfileList);
			producerService.putDataIntoKinesis(data);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok("Saved data into Kinessis sucessfully!");

	}
}
