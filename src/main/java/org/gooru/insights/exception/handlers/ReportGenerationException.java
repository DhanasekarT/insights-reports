package org.gooru.insights.exception.handlers;

/**
 * Simulated business-logic exception indicating a desired business logic failed.
 */
public class ReportGenerationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9198389632899336321L;

	public ReportGenerationException(){
		super();
	}
	
	//Overloaded Constructor for preserving the Message
	public ReportGenerationException(String msg) {
        super(msg);
    }
	
	//Overloaded Constructor for preserving the Message & cause
	public ReportGenerationException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
