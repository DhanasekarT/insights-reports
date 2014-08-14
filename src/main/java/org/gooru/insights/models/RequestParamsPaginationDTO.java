package org.gooru.insights.models;

import java.util.List;

public class RequestParamsPaginationDTO {

	private Integer offset;
	
	private Integer limit;
	
	private Integer totalRecords;

	private List<RequestParamsSortDTO> order;
	
	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	public Integer getLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public Integer getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(Integer totalRecords) {
		this.totalRecords = totalRecords;
	}

	public List<RequestParamsSortDTO> getOrder() {
		return order;
	}

	public void setOrder(List<RequestParamsSortDTO> order) {
		this.order = order;
	}
}
