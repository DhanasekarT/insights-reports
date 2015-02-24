package org.gooru.insights.models;

import java.util.List;
import java.util.Map;

public class ResponseParamDTO<M> {

	private List<M> content;
	
	private M message;
	
	private Map<String,Object> paginate;

	public List<M> getContent() {
		return content;
	}

	public void setContent(List<M> content) {
		this.content = content;
	}

	public M getMessage() {
		return message;
	}

	public void setMessage(M message) {
		this.message = message;
	}

	public Map<String, Object> getPaginate() {
		return paginate;
	}

	public void setPaginate(Map<String, Object> paginate) {
		this.paginate = paginate;
	}

	

}
