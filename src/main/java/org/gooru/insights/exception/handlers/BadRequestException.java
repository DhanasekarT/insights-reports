package org.gooru.insights.exception.handlers;

public class BadRequestException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5671697195864736310L;

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
