package org.gooru.insights.builders.utils;

import org.gooru.insights.constants.APIConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsightsLogger {

	private static final Logger logger = LoggerFactory.getLogger(InsightsLogger.class);

	private static final String TRACE_ID = "traceId: ";

	private static final String MESSAGE = "message: ";

	public static void error(String traceId, Exception exception){
		logger.error(buildMessage(traceId,null),exception);
	}
	
	public static void error(String traceId, String message, Exception exception){
		logger.error(buildMessage(traceId,message),exception);
	}
	
	public static void error(String traceId, String message){
		logger.error(buildMessage(traceId,message));
	}
	
	public static void debug(String traceId, Exception exception){
		logger.debug(buildMessage(traceId,null),exception);
	}
	
	public static void debug(String traceId, String message){
		logger.debug(buildMessage(traceId,message));
	}
	
	public static void info(String traceId, String message){
		logger.info(buildMessage(traceId,message));
	}
	
	private static String buildMessage(String traceId, String message){
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append(TRACE_ID);
		stringBuffer.append(traceId);
		if(message != null){
		stringBuffer.append(APIConstants.COMMA);
		stringBuffer.append(MESSAGE);
		stringBuffer.append(message);
		}
		return stringBuffer.toString();
	}
}
