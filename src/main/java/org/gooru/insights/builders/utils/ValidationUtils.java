package org.gooru.insights.builders.utils;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.exception.handlers.NotFoundException;
import org.springframework.validation.Errors;


public class ValidationUtils {

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorMsg) {
		if (StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorMsg);
		}
	}
	
	public static void rejectIfNullOrEmpty(Errors errors, Collection<?> data, String field, String errorMsg) {
		if (data == null || data.isEmpty()) {
			errors.rejectValue(field, errorMsg);
		}
	}
	
	public static void rejectIfNullOrEmpty(String data, String code, String... message) {
		if (StringUtils.isBlank(data)) {
			throw new BadRequestException(MessageHandler.getMessage(code, message));
		}
	}
	
	public static void rejectIfNullOrEmpty(Collection<?> data, String code, String... message) {
		if (data == null || data.isEmpty()) {
			throw new BadRequestException(MessageHandler.getMessage(code, message));
		}
	}
	
	public static void rejectIfFalse(Boolean data, String code, String... message) {
		if (!Boolean.valueOf(data)) {
			throw new BadRequestException(MessageHandler.getMessage(code, message));
		}
	}
	
	public static void rejectIfTrue(Boolean data, String code, String... message) {
		if (Boolean.valueOf(data)) {
			throw new BadRequestException(MessageHandler.getMessage(code, message));
		}
	}
	
	public static void rejectInvalidRequest(String code, String... message) {
		throw new BadRequestException(MessageHandler.getMessage(code, message));
	}
	
	public static void rejectIfNull(Object data, String code, String... message) {
		if (data == null) {
			throw new BadRequestException(MessageHandler.getMessage(code, message));
		}
	}
	
	public static void rejectIfNotFound(Object data, String code, String... message) {
		if (data == null) {
			throw new NotFoundException(MessageHandler.getMessage(code, message));
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
	
	public static void rejectIfMaxLimitExceed(int maxlimit, String content, String code, String... message) {
		if (content != null && content.length() > maxlimit) {
			throw new BadRequestException(MessageHandler.getMessage(code, message));
		}

	}
	
	public static void rejectIfAlreadyExist(Object data, String errorCode, String errorMsg) {
		if (data != null) {
			throw new BadRequestException(MessageHandler.getMessage(errorCode, errorMsg));
		}
	}

}

