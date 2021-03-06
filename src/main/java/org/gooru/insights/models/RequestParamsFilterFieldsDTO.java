package org.gooru.insights.models;

import java.io.Serializable;

public class RequestParamsFilterFieldsDTO implements Serializable {

	public RequestParamsFilterFieldsDTO() {

	}

	public RequestParamsFilterFieldsDTO(String operator, String fieldName, String value, String valueType, String type) {
		this.setFieldName(fieldName);
		this.setOperator(operator);
		this.setValue(value);
		this.setValueType(valueType);
		this.setType(type);
	}

	private static final long serialVersionUID = -2840599796987757919L;

	private String Operator;

	private String fieldName;

	private String value;

	private String valueType;

	private String type;

	private String from;

	private String to;

	private String format;
	
	private String dataSource;

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

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

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}


}
