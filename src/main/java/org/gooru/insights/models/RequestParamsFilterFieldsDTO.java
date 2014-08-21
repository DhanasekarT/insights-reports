package org.gooru.insights.models;

import java.io.Serializable;

import org.json.JSONObject;

public class RequestParamsFilterFieldsDTO implements Serializable{


	private static final long serialVersionUID = -2840599796987757919L;

	private String Operator;
	
	private String fieldName;
	
	private String value;
	
	private String valueType;
	
	private String type;

	private String from;
	
	private String to;
	
	public String getOperator() {
		return Operator;
	}

	public void setOperator(String operator) {
		Operator = operator;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getValueType() {
		return valueType;
	}

	public void setValueType(String valueType) {
		this.valueType = valueType;
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}
	
	public void setTo(String to) {
		this.to = to;
	}

	
}
