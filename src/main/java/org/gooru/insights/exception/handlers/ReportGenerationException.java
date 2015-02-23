package org.gooru.insights.exception.handlers;

/**
 * Simulated business-logic exception indicating a desired business logic failed.
 */
public class ReportGenerationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ReportGenerationException(String msg) {
        super(msg);
    }
}
