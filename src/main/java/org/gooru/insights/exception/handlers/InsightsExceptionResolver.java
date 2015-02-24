package org.gooru.insights.exception.handlers;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.Numbers;
import org.gooru.insights.models.ResponseParamDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

import flexjson.JSONSerializer;

public class InsightsExceptionResolver extends SimpleMappingExceptionResolver {

	private final Logger logger = LoggerFactory.getLogger(InsightsExceptionResolver.class);

	private final static String RESOLVER_DEBUG_MESSAGE = "Debug in Resolver:";
	
	private final static String RESOLVER_ERROR_MESSAGE = "Error in Resolver:";
	
	private final static String DEFAULT_ERROR = "DEFAULT_ERROR";


	/**
	 * This is an spring in-build function, we overriding it for throwing user
	 * defined exceptions
	 */
	@Override
	public ModelAndView doResolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {

		ResponseParamDTO<Map<Object, Object>> responseDTO = new ResponseParamDTO<Map<Object, Object>>();
		Map<Object, Object> errorMap = new HashMap<Object, Object>();
		Integer statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

		if (ex instanceof BadRequestException) {
			statusCode = HttpServletResponse.SC_BAD_REQUEST;
		} if (ex instanceof AccessDeniedException) {
			statusCode = HttpServletResponse.SC_FORBIDDEN;
		} else {
			statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}

		if (statusCode.toString().startsWith(Numbers.FOUR.getNumber())) {
			logger.debug(RESOLVER_DEBUG_MESSAGE, ex);
			errorMap.put(statusCode, ex.getMessage());
		} else if (statusCode.toString().startsWith(Numbers.FIVE.getNumber())) {
			logger.error(RESOLVER_ERROR_MESSAGE, ex);
			errorMap.put(statusCode, MessageHandler.getMessage(DEFAULT_ERROR));
		}

		response.setStatus(statusCode);
		responseDTO.setMessage(errorMap);
		return new ModelAndView(MessageHandler.getMessage(APIConstants.VIEW_NAME), MessageHandler.getMessage(APIConstants.RESPONSE_NAME), new JSONSerializer().exclude("*.class")
				.serialize(responseDTO));
	}

}
