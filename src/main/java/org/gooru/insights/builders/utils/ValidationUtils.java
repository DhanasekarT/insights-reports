package org.gooru.insights.builders.utils;

import java.util.Collection;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.exception.handlers.NotFoundException;
import org.springframework.validation.Errors;


public class ValidationUtils {

	private static ResourceBundle message = ResourceBundle.getBundle("localizer-properties/localeBundle_"+Locale.ENGLISH);

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorMsg) {
		if (StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorMsg);
		}
	}
	
	public static void rejectIfNullOrEmpty(Errors errors, Collection<?> data, String field, String errorMsg) {
		if (data != null && (!data.isEmpty())) {
			errors.rejectValue(field, errorMsg);
		}
	}
	
	public static void rejectIfNullOrEmpty(String data, String code, String... message) {
		if (StringUtils.isBlank(data)) {
			throw new BadRequestException(generateErrorMessage(code, message));
		}
	}
	
	public static void rejectIfNullOrEmpty(Collection<?> data, String code, String... message) {
		if (data != null && (!data.isEmpty())) {
			throw new BadRequestException(generateErrorMessage(code, message));
		}
	}
	
	public static void rejectIfFalse(Boolean data, String code, String... message) {
		if (!Boolean.valueOf(data)) {
			throw new BadRequestException(generateErrorMessage(code, message));
		}
	}
	
	public static void rejectIfTrue(Boolean data, String code, String... message) {
		if (Boolean.valueOf(data)) {
			throw new BadRequestException(generateErrorMessage(code, message));
		}
	}
	
	public static void rejectInvalidRequest(String code, String... message) {
		throw new BadRequestException(generateErrorMessage(code, message));
	}
	
	public static void rejectIfNull(Object data, String code, String... message) {
		if (data == null) {
			throw new BadRequestException(generateErrorMessage(code, message));
		}
	}
	
	public static void rejectIfNull(Object data, String code, int errorCode, String... message) {
		if (data == null) {
			if (errorCode == 404) {
				throw new NotFoundException(generateErrorMessage(code, message));
			} else { 
				throw new BadRequestException(generateErrorMessage(code, message));
			}
		}
	}

	public static void rejectIfNull(Errors errors, Object data, String field, String errorMsg) {
		if (data == null) {
			errors.rejectValue(field, errorMsg);
		}
	}

	public static void rejectIfNull(Errors errors, Object data, String field, String errorCode, String errorMsg) {
		if (data == null) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfAlReadyExist(Errors errors, Object data, String errorCode, String errorMsg) {
		if (data != null) {
			errors.reject(errorCode, errorMsg);
		}
	}

	public static void rejectIfNullOrEmpty(Errors errors, Set<?> data, String field, String errorMsg) {
		if (data == null || data.size() == 0) {
			errors.rejectValue(field, errorMsg);
		}
	}

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorCode, String errorMsg) {
		if (StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfInvalidDate(Errors errors, Date data, String field, String errorCode, String errorMsg) {
		Date date = new Date();
		if (data.compareTo(date) <= 0) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfNotValid(Errors errors, Integer data, String field, String errorCode, String errorMsg, Integer maxValue) {
		if (data <= 0 || data > maxValue) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static String generateErrorMessage(String errorCode) {
		return message.getString(errorCode);
	}

	public static String generateErrorMessage(String errorCode, String... params) {
		String errorMsg = message.getString(errorCode);
		if (params != null) {
			for (int index = 0; index < params.length; index++) {
				errorMsg = errorMsg.replace("{" + index + "}", params[index]);
			}
		}
		return errorMsg;
	}

	public static String generateMessage(String code, String... params) {
		String msg = message.getString(code);
		if (params != null) {
			for (int index = 0; index < params.length; index++) {
				msg = msg.replace("{" + index + "}", params[index] == null ? "" : params[index]);
			}
		}
		return msg;
	}
	
	public static String generateMessage(String rawData, Map<String, Object> data) {
		if (rawData != null && data != null) {
			for (Map.Entry<String, Object> entry : data.entrySet()) {
			    rawData = rawData.replace("[" + entry.getKey() + "]", entry.getValue() == null ? "" : (String)entry.getValue());
			}
		}
		return rawData;
	}
	
	public static void rejectIfMaxLimitExceed(int maxlimit, String content, String code, String... message) {
		if (content != null && content.length() > maxlimit) {
			throw new BadRequestException(generateErrorMessage(code, message));
		}

	}
	
	public static void rejectIfAlreadyExist(Object data, String errorCode, String errorMsg) {
		if (data != null) {
			throw new BadRequestException(generateErrorMessage(errorCode, errorMsg));
		}
	}

}

