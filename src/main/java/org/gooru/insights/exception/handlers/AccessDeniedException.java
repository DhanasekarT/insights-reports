package org.gooru.insights.exception.handlers;

public class AccessDeniedException extends RuntimeException{

	public AccessDeniedException(){
		super();
	}
	
	//Overloaded Constructor for preserving the Message
	public AccessDeniedException(String message){
		super(message);
	}
}
