package org.gooru.insights.controllers;

import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.models.ResponseParamDTO;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

import flexjson.JSONSerializer;

@Controller
public class BaseController extends APIConstants{

private static final String REQUEST_DATA_ERROR = "Include data parameter!!!";
	
	private static final String REQUEST_BODY_ERROR = "Include JSON Body data!!!";
	
	
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
		jsonObject.put("url", request.getRequestURL().toString());
		UUID uuid = UUID.randomUUID();
		InsightsLogger.debug(uuid.toString(), jsonObject.toString());
		request.setAttribute("traceId", uuid.toString());
		return uuid.toString();
	}

	public String getRequestData(HttpServletRequest request,String requestBody){
		if(request.getMethod().equalsIgnoreCase("GET")){
			if(request.getParameter("data") == null){
				throw new BadRequestException(REQUEST_DATA_ERROR);
			}
			return request.getParameter("data").toString();
		}else {
			if(requestBody == null){
				throw new BadRequestException(REQUEST_BODY_ERROR);
			}
			return requestBody;
		}
	}
	
	public HttpServletResponse setAllowOrigin(HttpServletResponse response) {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE");
		return response;
	}
}
