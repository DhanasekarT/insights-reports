package org.gooru.insights.models;

import java.io.Serializable;

public class RequestParamsDTO implements Serializable {

	private static final long serialVersionUID = -2840599796987757919L;

	private String fields;
	
	private String fileName;
	
	private String dataSource;
	
	RequestParamsPaginationDTO paginate;
	
	RequestParamsFiltersDTO filters;
	
	private String groupBy;

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public RequestParamsFiltersDTO getFilters() {
		return filters;
	}

	public void setFilters(RequestParamsFiltersDTO filters) {
		this.filters = filters;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	public RequestParamsPaginationDTO getPaginate() {
		return paginate;
	}

	public void setPaginate(RequestParamsPaginationDTO paginate) {
		this.paginate = paginate;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	
	
	
}
