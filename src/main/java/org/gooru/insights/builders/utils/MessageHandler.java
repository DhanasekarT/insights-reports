package org.gooru.insights.builders.utils;

import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import javax.annotation.PostConstruct;

import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.services.BaseAPIServiceImpl;
import org.gooru.insights.services.BaseConnectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MessageHandler {
	
	private static final String LOCALIZER = "localizer";
	
	private static final String DEFAULT_MESSAGE = "message not found!";

	private static final String DEFAULT_LOCALIZER_LOCATION = "localizer-properties/localeBundle_"+Locale.ENGLISH;
	
	private static ResourceBundle resourceBundle = null;
	
	@Autowired
	private BaseConnectionService baseConnectionService;
	
	@PostConstruct
	private void init(){
		resourceBundle = ResourceBundle.getBundle(baseConnectionService.getFileProperties().containsKey(LOCALIZER) ? 
				baseConnectionService.getFileProperties().get(LOCALIZER).toString() : DEFAULT_LOCALIZER_LOCATION);
	}

	/**
	 * This will provide the value in the localizer
	 * @param key will be the fetch key
	 * @return value returned as string
	 */
	public static String getMessage(String key) {
		
		if(resourceBundle.containsKey(key)){
			return resourceBundle.getString(key);
		}
		return DEFAULT_MESSAGE;
	}
	/**
	 * 
	 * @param key
	 * @param replacer
	 * @return
	 */
	public static String getMessage(String key,String... replacer ) {
		
		if(resourceBundle.containsKey(key)){
			String value = resourceBundle.getString(key);
			for(int i =0;i < replacer.length; i++){
				value = value.replace(BaseAPIServiceImpl.buildString(new Object[]{APIConstants.OPEN_BRACE,i,APIConstants.CLOSE_BRACE}), replacer[i]);
			}
			return value;
		}
		return DEFAULT_MESSAGE;
	}
	
	public static String generateErrorMessage(String errorCode) {
		return resourceBundle.getString(errorCode);
	}

	public static String generateErrorMessage(String errorCode, String... params) {
		String errorMsg = resourceBundle.getString(errorCode);
		if (params != null) {
			for (int index = 0; index < params.length; index++) {
				errorMsg = errorMsg.replace(APIConstants.OPEN_BRACE + index + APIConstants.CLOSE_BRACE, params[index]);
			}
		}
		return errorMsg;
	}

	public static String generateMessage(String code, String... params) {
		String msg = resourceBundle.getString(code);
		if (params != null) {
			for (int index = 0; index < params.length; index++) {
				msg = msg.replace(APIConstants.OPEN_BRACE + index + APIConstants.CLOSE_BRACE, params[index] == null ? APIConstants.EMPTY : params[index]);
			}
		}
		return msg;
	}
	
	public static String generateMessage(String rawData, Map<String, Object> data) {
		if (rawData != null && data != null) {
			for (Map.Entry<String, Object> entry : data.entrySet()) {
			    rawData = rawData.replace(APIConstants.OPEN_SQUARE_BRACKET + entry.getKey() + APIConstants.CLOSE_SQUARE_BRACKET, entry.getValue() == null ? APIConstants.EMPTY : (String)entry.getValue());
			}
		}
		return rawData;
	}
}
