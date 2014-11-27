package org.gooru.insights.exception.handlers;

public class BadRequestException extends RuntimeException {
	
	public BadRequestException()
	{
		super();
	}

	//Overloaded Constructor for preserving the Message
	public BadRequestException(String msg) {
		super(msg);
	}

	//Overloaded Constructor for preserving the Message & cause
	public BadRequestException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
