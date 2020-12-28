package com.example.restservice;

public class Student {

	private final long studentId;
	private final String name;

	public Student(long studentId, String name) {
		this.studentId = studentId;
		this.name = name;
	}

	public long getStudentId() {
		return studentId;
	}

	public String getName() {
		return name;
	}
}
