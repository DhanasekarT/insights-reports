package org.gooru.insights.services;

import java.util.Collection;
import java.util.Map;

import org.gooru.insights.models.RequestParamsDTO;

public interface BaseAPIService {

	RequestParamsDTO buildRequestParameters(String data);
	boolean checkNull(Collection<?> request);
	
	boolean checkNull(String request);
	
	String[] convertStringtoArray(String data);
	
	boolean checkNull(Map<?,?> request);
	
	boolean checkNull(Integer parameter);
}
