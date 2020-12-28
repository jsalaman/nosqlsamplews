package com.example.restservice;

public class Status {
	String status="ok";
	String comment="lalal";
	
			
	public Status(String status, String comment) {
		this.status = status;
		this.comment = comment;
	}

	public String getStatus() {
		return status;
	}

	public String getComment() {
		return comment;
	}
}