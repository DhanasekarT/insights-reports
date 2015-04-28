package org.gooru.insights.exception.handlers;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.Numbers;
import org.gooru.insights.models.ResponseParamDTO;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

import flexjson.JSONSerializer;

public class InsightsExceptionResolver extends SimpleMappingExceptionResolver {

	private static final String DEFAULT_ERROR = "Internal Server Error!!!";
	
	private static final String DEVELOPER_MESSAGE = "developerMessage";

	private static final String MAIL_To = "mailTo";

	private static final String SUPPORT_EMAIL_ID = "support@goorulearning.org";

	private static final String STATUS_CODE = "statusCode";
	
	private static final String DEFAULT_TRACEID = "traceId Not-Specified";


	/**
	 * This is an spring in-build function, we overriding it for throwing user
	 * defined exceptions
	 */
	@Override
	public ModelAndView doResolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {

		ResponseParamDTO<Map<Object, Object>> responseDTO = new ResponseParamDTO<Map<Object, Object>>();
		Map<Object, Object> errorMap = new HashMap<Object, Object>();
		Integer statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		String traceId = request.getAttribute(APIConstants.TRACE_ID) != null ? request.getAttribute(APIConstants.TRACE_ID).toString() : DEFAULT_TRACEID;
		
		if (ex instanceof BadRequestException) {
			statusCode = HttpServletResponse.SC_BAD_REQUEST;
		}else if (ex instanceof AccessDeniedException) {
			statusCode = HttpServletResponse.SC_FORBIDDEN;
		} else {
			statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}

		if (statusCode.toString().startsWith(Numbers.FOUR.getNumber())) {
			InsightsLogger.debug(traceId, ex);
			errorMap.put(DEVELOPER_MESSAGE, ex.getMessage());
		} else if (statusCode.toString().startsWith(Numbers.FIVE.getNumber())) {
			InsightsLogger.error(traceId, ex);
			errorMap.put(DEVELOPER_MESSAGE, DEFAULT_ERROR);
		}
		errorMap.put(STATUS_CODE, statusCode);
		errorMap.put(MAIL_To, SUPPORT_EMAIL_ID);

		response.setStatus(statusCode);
		responseDTO.setMessage(errorMap);
		return new ModelAndView(MessageHandler.getMessage(APIConstants.VIEW_NAME), MessageHandler.getMessage(APIConstants.RESPONSE_NAME), new JSONSerializer().exclude("*.class")
				.serialize(responseDTO));
	}

}
