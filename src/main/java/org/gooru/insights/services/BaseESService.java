package org.gooru.insights.services;

import java.util.Map;

import org.gooru.insights.constants.ResponseParamDTO;
import org.gooru.insights.models.RequestParamsDTO;

public interface BaseESService {

	ResponseParamDTO<Map<String,Object>> generateQuery(RequestParamsDTO requestParamsDTO,
			String[] indices,Map<String,Boolean> validatedData) throws Exception;

}
