package org.gooru.insights.exception.handlers;

public class NotFoundException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4623950247986004914L;

	public NotFoundException()
	{
		super();
	}

	//Overloaded Constructor for preserving the Message
	public NotFoundException(String msg) {
		super(msg);
	}

	//Overloaded Constructor for preserving the Message & cause
	public NotFoundException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
