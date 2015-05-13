package org.gooru.insights.exception.handlers;

public class AccessDeniedException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2461084508244996433L;

	public AccessDeniedException(){
		super();
	}
	
	//Overloaded Constructor for preserving the Message
	public AccessDeniedException(String message){
		super(message);
	}
}
