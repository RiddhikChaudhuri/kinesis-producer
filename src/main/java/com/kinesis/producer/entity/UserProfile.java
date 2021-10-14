package com.kinesis.producer.entity;

import java.util.Date;

import javax.validation.constraints.Email;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserProfile {

	private Long userId;

	private String firstName;

	private String lastName;

	private Date dateOfBirth;

	private Long mobileNumber;

	@Email
	private String email;

	private String pinCode;

	private String country;

	private String state;

	private String city;

	private String nationality;
}
