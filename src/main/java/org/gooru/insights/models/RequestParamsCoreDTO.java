package org.gooru.insights.models;

import java.io.Serializable;
import java.util.List;

public class RequestParamsCoreDTO implements Serializable{

	private static final long serialVersionUID = -2840599796987757919L;
	
	List<RequestParamsDTO> api;
	
	String apiFunctionality;
	
	String coreKey;

	public String getCoreKey() {
		return coreKey;
	}

	public void setCoreKey(String coreKey) {
		this.coreKey = coreKey;
	}

	public String getApiFunctionality() {
		return apiFunctionality;
	}

	public void setApiFunctionality(String apiFunctionality) {
		this.apiFunctionality = apiFunctionality;
	}

	public List<RequestParamsDTO> getRequestParamsDTO() {
		return api;
	}

	public void setRequestParamsDTO(List<RequestParamsDTO> api) {
		this.api = api;
	}
}
