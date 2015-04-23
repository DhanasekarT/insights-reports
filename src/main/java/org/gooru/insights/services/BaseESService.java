package org.gooru.insights.services;

import java.util.Map;

import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.ResponseParamDTO;

public interface BaseESService {

	ResponseParamDTO<Map<String,Object>> generateQuery(String traceId,RequestParamsDTO requestParamsDTO,
			String[] indices,Map<String,Boolean> validatedData) throws Exception;

}
