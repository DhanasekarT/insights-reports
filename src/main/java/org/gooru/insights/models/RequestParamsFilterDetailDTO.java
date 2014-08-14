package org.gooru.insights.models;

public class RequestParamsFilterDetailDTO {

	private String logicalOperatorPrefix;
	
	private RequestParamsFilterFieldsDTO requestParamsFilterFieldsDTO;

	public String getLogicalOperatorPrefix() {
		return logicalOperatorPrefix;
	}

	public void setLogicalOperatorPrefix(String logicalOperatorPrefix) {
		this.logicalOperatorPrefix = logicalOperatorPrefix;
	}


	public RequestParamsFilterFieldsDTO getRequestParamsFilterFieldsDTO() {
		return requestParamsFilterFieldsDTO;
	}

	public void setRequestParamsFilterFieldsDTO(
			RequestParamsFilterFieldsDTO requestParamsFilterFieldsDTO) {
		this.requestParamsFilterFieldsDTO = requestParamsFilterFieldsDTO;
	}
}
