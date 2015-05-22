package org.gooru.insights.controllers;

import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.models.ResponseParamDTO;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

import flexjson.JSONSerializer;

@Controller
public class BaseController extends APIConstants{

	private static final String DATA = "data";
	
	public <M> ModelAndView getModel(ResponseParamDTO<M> data) {

		ModelAndView model = new ModelAndView(MessageHandler.getMessage(APIConstants.VIEW_NAME));
		model.addObject(MessageHandler.getMessage(APIConstants.RESPONSE_NAME), new JSONSerializer().exclude(EXCLUDE_CLASSES).deepSerialize(data));
		return model;
	}
	
	public String getTraceId(HttpServletRequest request) throws JSONException {
		Map<String, Object> requestParam = request.getParameterMap();
		JSONObject jsonObject = new JSONObject();
		for (Map.Entry<String, Object> entry : requestParam.entrySet()) {
			jsonObject.put(entry.getKey(), entry.getValue());
		}
		jsonObject.put(URL, request.getRequestURL().toString());
		UUID uuid = UUID.randomUUID();
		InsightsLogger.debug(uuid.toString(), jsonObject.toString());
		request.setAttribute(TRACE_ID, uuid.toString());
		return uuid.toString();
	}

	public String getSessionToken(HttpServletRequest request) {

		if (request.getHeader(GOORU_SESSION_TOKEN) != null) {
			return request.getHeader(GOORU_SESSION_TOKEN);
		} else {
			return request.getParameter(SESSION_TOKEN);
		}
	}
	
	public String getRequestData(HttpServletRequest request,String requestBody){
		
		if(request.getParameter(DATA) != null) {
			requestBody = request.getParameter(DATA);
		}else if(requestBody == null || requestBody.isEmpty()){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100, DATA));
		}
			return requestBody;
	}
	
	public HttpServletResponse setAllowOrigin(HttpServletResponse response) {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE");
		return response;
	}
}
