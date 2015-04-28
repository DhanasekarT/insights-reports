package org.gooru.insights.models;

import java.io.Serializable;


public class RequestParamsRangeDTO implements Serializable {

	private static final long serialVersionUID = -2840599796984637919L;
	
	private double from;
	
	private double to;

	public double getFrom() {
		return from;
	}

	public void setFrom(double from) {
		this.from = from;
	}

	public double getTo() {
		return to;
	}

	public void setTo(double to) {
		this.to = to;
	}
	
	
}
