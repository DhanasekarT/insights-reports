package org.gooru.insights.services;

import java.util.Collection;

import org.gooru.insights.models.RequestParamsDTO;

public interface BaseAPIService {

	RequestParamsDTO buildRequestParameters(String data);
	boolean checkNull(Collection<?> request);
	
	boolean checkNull(String request);
	
	String[] convertStringtoArray(String data);
}
