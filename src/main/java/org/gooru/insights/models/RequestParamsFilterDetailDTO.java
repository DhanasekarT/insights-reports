package org.gooru.insights.models;

import java.io.Serializable;
import java.util.List;

public class RequestParamsFilterDetailDTO implements Serializable{


	private static final long serialVersionUID = -2840599796987757919L;

	private String logicalOperatorPrefix;
	
	private List<RequestParamsFilterFieldsDTO> fields;

	public List<RequestParamsFilterFieldsDTO> getFields() {
		return fields;
	}

	public void setFields(
			List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTO) {
		this.fields = requestParamsFilterFieldsDTO;
	}

	public String getLogicalOperatorPrefix() {
		return logicalOperatorPrefix;
	}

	public void setLogicalOperatorPrefix(String logicalOperatorPrefix) {
		this.logicalOperatorPrefix = logicalOperatorPrefix;
	}
}
