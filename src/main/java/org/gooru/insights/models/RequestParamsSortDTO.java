package org.gooru.insights.models;

import java.io.Serializable;

public class RequestParamsSortDTO implements Serializable{


	private static final long serialVersionUID = -2840599796987757919L;

	private String sortBy;
	
	private String sortOrder;

	public String getSortBy() {
		return sortBy;
	}

	public void setSortBy(String sortBy) {
		this.sortBy = sortBy;
	}

	public String getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(String sortOrder) {
		this.sortOrder = sortOrder;
	}

	
}
