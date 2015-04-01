package org.gooru.insights.builders.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsightsLogger {

	private static final Logger logger = LoggerFactory.getLogger(InsightsLogger.class);

	private static final String TRACE_ID = "traceId: ";

	private static final String METHOD_NAME = "methodName: ";

	private static final String EXCEPTION = "exception: ";
	
	private static final String MESSAGE = "message: ";

	private static final String CLASS_NAME = "className: ";

	private static final String LINE_NO = "lineNumber: ";
	
	private static final String COMMA = ",";
	
	public static void error(String traceId,Exception exception){
		logger.error(TRACE_ID+traceId+COMMA+EXCEPTION+exception+COMMA+convertArrayToString(exception.getStackTrace()));
	}
	
	public static void error(String traceId,String message,Exception exception){
		logger.error(TRACE_ID+traceId+COMMA+MESSAGE+message+COMMA+EXCEPTION+exception+COMMA+convertArrayToString(exception.getStackTrace()));
	}
	
	public static void error(String traceId,String msg){
		logger.error(TRACE_ID+traceId+COMMA+MESSAGE+msg);
	}
	
	public static void debug(String traceId,Exception exception){
		logger.debug(TRACE_ID+traceId+COMMA+EXCEPTION+exception);
	}
	
	public static void debug(String traceId,String msg){
		logger.debug(TRACE_ID+traceId+COMMA+MESSAGE+msg);
	}
	
	public static void info(String traceId,String msg){
		logger.debug(TRACE_ID+traceId+COMMA+MESSAGE+msg);
	}
	
	private static String convertArrayToString(StackTraceElement[] stackTraceElements){
		StringBuffer stringBuffer = new StringBuffer();
		for(int i=0;i< stackTraceElements.length;i++){
			if(!stackTraceElements[i].getClassName().contains("org.gooru.insights.api")){
				continue;
			}
			if(stringBuffer.length() > 0){
				stringBuffer.append("->");
			}
			stringBuffer.append(CLASS_NAME+stackTraceElements[i].getFileName()+COMMA+METHOD_NAME+stackTraceElements[i].getMethodName()+COMMA+LINE_NO+stackTraceElements[i].getLineNumber());
		}
		return stringBuffer.toString();
	}
}
