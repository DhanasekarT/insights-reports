package org.gooru.insights.exception.handlers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

import flexjson.JSONSerializer;

public class InsightsExceptionResolver extends SimpleMappingExceptionResolver {

	private final Logger logger = LoggerFactory.getLogger(InsightsExceptionResolver.class);

	@Override
	public ModelAndView doResolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
		ErrorObject errorObject = null;
		boolean isLogError = false;
		if (ex instanceof AccessDeniedException) {
			errorObject = new ErrorObject(403, ex.getMessage());
			response.setStatus(403);
		} else {
			errorObject = new ErrorObject(500, "Internal Server Error");
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
		
		if(!isLogError)
		{
			logger.debug("Error in Resolver -- ", ex);
		}
		ModelAndView jsonModel = new ModelAndView("rest/model");
		jsonModel.addObject("model", new JSONSerializer().exclude("*.class").serialize(errorObject));
		return jsonModel;
	}

}


