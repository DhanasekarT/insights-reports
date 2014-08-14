package org.gooru.insights.models;

import java.io.Serializable;

import org.json.JSONArray;

public class RequestParamsDTO implements Serializable {

	private static final long serialVersionUID = -2840599796987757919L;

	private String fields;
	
	private String fileName;
	
	private String dataSource;
	
	RequestParamsPaginationDTO pagination;
	
	RequestParamsFiltersDTO filters;
	
	private JSONArray aggregations;
	
	private String intervals;
	
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

	public RequestParamsPaginationDTO getPagination() {
		return pagination;
	}

	public void setPagination(RequestParamsPaginationDTO paginate) {
		this.pagination = paginate;
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

	public JSONArray getAggregations() {
		return aggregations;
	}

	public void setAggregations(JSONArray aggregations) {
		this.aggregations = aggregations;
	}

	public String getIntervals() {
		return intervals;
	}

	public void setIntervals(String intervals) {
		this.intervals = intervals;
	}
	
	
	
}
