package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.builders.utils.ExcludeNullTransformer;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.DataTypes;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import flexjson.JSONSerializer;

@Component
public class UserServiceImpl implements UserService {

	@Autowired
	private ESDataProcessor businessLogicService;
	
	public Map<String, Object> getUserFilters(String gooruUId) {

		final Map<String, Object> allowedFilters = new HashMap<String, Object>();

		allowedFilters.put(APIConstants.CREATORUID, gooruUId);
		allowedFilters.put(APIConstants.GOORUUID, gooruUId);
		allowedFilters.put(APIConstants.CREATOR_UID, gooruUId);
		allowedFilters.put(APIConstants.GOORU_UID, gooruUId);
		allowedFilters.put(APIConstants.USERUID, gooruUId);

		return allowedFilters;
	}

	public Map<String, Object> getUserFiltersAndValues(List<RequestParamsFilterDetailDTO> filters) {
		Map<String, Object> userFiltersValue = new HashMap<String, Object>();
		Set<String> userValue = new HashSet<String>();
		Set<String> orgValue = new HashSet<String>();
		if (filters != null) {
			
			for (RequestParamsFilterDetailDTO fieldData : filters) {
				for (RequestParamsFilterFieldsDTO fieldsDetails : fieldData.getFields()) {
					Set<Object> values = new HashSet<Object>();
					for (String value : fieldsDetails.getValue().split(APIConstants.COMMA)) {
						values.add(value);
						userFiltersValue.put(fieldsDetails.getFieldName(), values);

						if (fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.CONTENTORGUID) || fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.CONTENT_ORG_UID)
								|| fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.USERORGID) || fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.USER_ORG_UID)) {
							orgValue.add(value);
						}
						if (fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.CREATORUID) || fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.CREATOR_UID)
								|| fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.GOORUUID) || fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.GOORU_UID)
								|| fieldsDetails.getFieldName().equalsIgnoreCase(APIConstants.USERUID)) {
							userValue.add(value);
						}
					}
				}
			}
		}
		userFiltersValue.put(APIConstants.ORG_FILTERS, orgValue);
		userFiltersValue.put(APIConstants.USER_FILTERS, userValue);

		return userFiltersValue;
	}

	public Map<Integer, String> checkIfFieldValueMatch(Map<String, Object> allowedFilters, Map<String, Object> userFilters, Map<Integer, String> errorMap) {
		boolean notAllow = false;
		boolean isFilterAvailable = false;
		for (Map.Entry<String, Object> entry : allowedFilters.entrySet()) {
			if (userFilters.containsKey(entry.getKey())) {
				isFilterAvailable = true;
				Set<Object> values = (Set<Object>) userFilters.get(entry.getKey());
				if (entry.getValue() instanceof String && !values.contains(entry.getValue())) {
					notAllow = true;
					break;
				}
				if (entry.getValue() instanceof Set<?>) {
					for (Object val : (Set<Object>) entry.getValue()) {
						if (!values.contains(val)) {
							notAllow = true;
							break;
						}
					}
				}
			}
		}
		if (notAllow) {
			 errorMap.put(403, MessageHandler.getMessage(ErrorConstants.E104, APIConstants.EMPTY));
//			throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, APIConstants.EMPTY));
		}
		/*
		 * if(isFilterAvailable && !notAllow){ errorMap.put(200, E1012); }
		 */
		return errorMap;
	}

	public RequestParamsDTO userPreValidation(RequestParamsDTO requestParamsDTO, Set<String> userFilterUserValues, Map<String, Set<String>> partyPermissions) {
		for (String userFilterUserValue : userFilterUserValues) {
			if (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES)) {
				if (!(partyPermissions.containsKey(userFilterUserValue) && partyPermissions.get(userFilterUserValue).contains(APIConstants.AP_PARTY_OWN_CONTENT_USAGE))) {
					return requestParamsDTO;
				}
			}
			if ((requestParamsDTO.getDataSource().matches(APIConstants.CONTENTDATASOURCES) && !requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES))) {
				if (!(partyPermissions.containsKey(userFilterUserValue) && partyPermissions.get(userFilterUserValue).contains(APIConstants.AP_PARTY_OWN_CONTENT_USAGE))) {
					return requestParamsDTO;
				}
			}
		}
		return requestParamsDTO;
	}

	public RequestParamsDTO validateOrganization(RequestParamsDTO requestParamsDTO, Map<String, Set<String>> partyPermissions, Map<Integer, String> errorMap, Set<String> userFilterOrgValues) {
		for (String userFilterOrgValue : userFilterOrgValues) {
			if (requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(APIConstants.AP_PARTY_PII))) {
					return getBusinessLogicService().changeDataSourceUserToAnonymousUser(requestParamsDTO);
//					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_PII));
				}
			} else if ((requestParamsDTO.getDataSource().matches(APIConstants.CONTENTDATASOURCES) || requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES))
					&& !StringUtils.isBlank(requestParamsDTO.getGroupBy()) && requestParamsDTO.getGroupBy().matches(APIConstants.USERFILTERPARAM)) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(APIConstants.AP_PARTY_PII))) {
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_PII));
				}
			} else if (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy())) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(APIConstants.AP_PARTY_ACTIVITY_RAW))) {
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_RAW));
				}
			} else if (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) && !StringUtils.isBlank(requestParamsDTO.getGroupBy())) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(APIConstants.AP_PARTY_ACTIVITY))) {
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_ACTIVITY));
				}
			}
		}
		return requestParamsDTO;
	}

	public List<RequestParamsFilterDetailDTO> addSystemContentUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {
		
		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemContentFields = new RequestParamsFilterFieldsDTO(APIConstants.IN, APIConstants.CONTENTORGUID, userOrgUId, DataTypes.STRING.dataType(), APIConstants.SELECTOR);
		userFilters.add(systemContentFields);
		systeFilterDetails.setLogicalOperatorPrefix(APIConstants.OR);
		RequestParamsFilterFieldsDTO systemUserFields = new RequestParamsFilterFieldsDTO(APIConstants.IN, APIConstants.USERORGID, userOrgUId, DataTypes.STRING.dataType(), APIConstants.SELECTOR);
		userFilters.add(systemUserFields);
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public List<RequestParamsFilterDetailDTO> addSystemContentOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {
	
		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemContentFields = new RequestParamsFilterFieldsDTO(APIConstants.IN, APIConstants.CONTENTORGUID, userOrgUId, DataTypes.STRING.dataType(), APIConstants.SELECTOR);
		userFilters.add(systemContentFields);
		systeFilterDetails.setLogicalOperatorPrefix(APIConstants.OR);
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public List<RequestParamsFilterDetailDTO> addSystemUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {

		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemUserFields = new RequestParamsFilterFieldsDTO(APIConstants.IN, APIConstants.USERORGID, userOrgUId, DataTypes.STRING.dataType(), APIConstants.SELECTOR);
		userFilters.add(systemUserFields);
		systeFilterDetails.setLogicalOperatorPrefix(APIConstants.OR);
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public String getRoleBasedParty(String traceId,Map<String, Set<String>> partyPermissions, String permission) {
		StringBuilder allowedOrg = new StringBuilder();
		for (Map.Entry<String, Set<String>> entry : partyPermissions.entrySet()) {
			if (entry.getValue().contains(permission)) {
				allowedOrg.append(allowedOrg.length() == 0 ? APIConstants.EMPTY : APIConstants.COMMA);
				allowedOrg.append(entry.getKey().toString());
			}
		}
		if (allowedOrg.length() == 0 && !permission.matches(APIConstants.RESTRICTEDPERMISSION)) {
			allowedOrg.append(APIConstants.DEFAULTORGUID);
		}
		InsightsLogger.info(traceId, APIConstants.ALLOWED_ORG+allowedOrg.toString());
		return allowedOrg.toString();
	}

	public String getAllowedParties(String traceId,RequestParamsDTO requestParamsDTO, Map<String, Set<String>> partyPermissions) {
		String allowedParty = null;
		if ((requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)) || (requestParamsDTO.getDataSource().matches(APIConstants.CONTENTDATASOURCES) || requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES)) && !StringUtils.isBlank(requestParamsDTO.getGroupBy())
				&& requestParamsDTO.getGroupBy().matches(APIConstants.USERFILTERPARAM)) {
			allowedParty = APIConstants.AP_PARTY_PII;
		} else if (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy())) {
			allowedParty = APIConstants.AP_PARTY_ACTIVITY_RAW;
		} else if (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES)) {
			allowedParty = APIConstants.AP_PARTY_ACTIVITY;
		} else if (requestParamsDTO.getDataSource().matches(APIConstants.CONTENTDATASOURCES)) {
			allowedParty = APIConstants.AP_PARTY_OWN_CONTENT_USAGE;
		}
		return getRoleBasedParty(traceId,partyPermissions, allowedParty);
	}

	public RequestParamsDTO validateUserRole(String traceId,RequestParamsDTO requestParamsDTO, Map<String, Object> userMap) {
		
		String gooruUId = userMap.containsKey(APIConstants.GOORUUID) ? userMap.get(APIConstants.GOORUUID).toString() : null;

		Map<Integer,String> errorMap = new HashMap<Integer, String>();
		Map<String, Set<String>> partyPermissions = (Map<String, Set<String>>) userMap.get(APIConstants.PERMISSIONS);
		InsightsLogger.info(traceId,APIConstants.GOORUUID+APIConstants.SEPARATOR+gooruUId);
		InsightsLogger.info(traceId,APIConstants.PERMISSIONS+APIConstants.SEPARATOR+partyPermissions);
		
		if(!StringUtils.isBlank(getRoleBasedParty(traceId,partyPermissions,APIConstants.AP_ALL_PARTY_ALL_DATA))){
			return requestParamsDTO;
		}

		Map<String, Object> userFilters = getUserFilters(gooruUId);
		Map<String, Object> userFiltersAndValues = getUserFiltersAndValues(requestParamsDTO.getFilter());
		Set<String> userFilterOrgValues = (Set<String>) userFiltersAndValues.get("orgFilters");
		Set<String> userFilterUserValues = (Set<String>) userFiltersAndValues.get("userFilters");

		String partyAlldataPerm = getRoleBasedParty(traceId,partyPermissions,APIConstants.AP_PARTY_ALL_DATA);
		
		if(!StringUtils.isBlank(partyAlldataPerm) && userFilterOrgValues.isEmpty()){			
			addSystemContentUserOrgFilter(requestParamsDTO.getFilter(), partyAlldataPerm);
		}
		if(!StringUtils.isBlank(partyAlldataPerm) && !userFilterOrgValues.isEmpty()){			
			for(String org : userFilterOrgValues){
				if(!partyAlldataPerm.contains(org)){
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E108));
				}
			}		
			return requestParamsDTO;
		}
		
		Map<String, Object> orgFilters = new HashMap<String, Object>();
		
		for(Entry<String, Set<String>> e : partyPermissions.entrySet()){
			if(e.getValue().contains(APIConstants.AP_ALL_PARTY_ALL_DATA)){
				return requestParamsDTO;
			}else if(e.getValue().contains(APIConstants.AP_PARTY_ALL_DATA)){
				orgFilters.put(e.getKey(), e.getValue());
			}
		}
		if(userFilterOrgValues.isEmpty() && !orgFilters.isEmpty()){
			return requestParamsDTO;
		}
		
		if (!checkIfFieldValueMatch(userFilters, userFiltersAndValues, errorMap).isEmpty()) {
			if(errorMap.containsKey(403)){
				return userPreValidation(requestParamsDTO, userFilterUserValues, partyPermissions);
			}else{
				errorMap.clear();
				return requestParamsDTO;
			}
		}

		if (partyPermissions.isEmpty() && (requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)|| (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) 
				&& !StringUtils.isBlank(requestParamsDTO.getGroupBy()) && requestParamsDTO.getGroupBy().matches(APIConstants.USERFILTERPARAM)))) {
//			throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_PII));
			return businessLogicService.changeDataSourceUserToAnonymousUser(requestParamsDTO);
		}
		if (partyPermissions.isEmpty() && (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy()))) {
			errorMap.put(403,MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_RAW));
			throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_RAW));
		}

		if (!userFilterOrgValues.isEmpty()) {
			validateOrganization(requestParamsDTO, partyPermissions, errorMap, userFilterOrgValues);
		} else {
			String allowedParty = getAllowedParties(traceId,requestParamsDTO, partyPermissions);
			if (!StringUtils.isBlank(allowedParty)) {
				if(requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)){
					addSystemUserOrgFilter(requestParamsDTO.getFilter(), allowedParty);
				}else{
					addSystemContentUserOrgFilter(requestParamsDTO.getFilter(), allowedParty);
				}
			} else {
				throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E108));
			}
		}

		JSONSerializer serializer = new JSONSerializer();
		serializer.transform(new ExcludeNullTransformer(), void.class).exclude(APIConstants.EXCLUDE_CLASSES);
		InsightsLogger.info(traceId,APIConstants.NEW_QUERY+serializer.deepSerialize(requestParamsDTO));
		return requestParamsDTO;
	}
	public ESDataProcessor getBusinessLogicService() {
		return businessLogicService;
	}

}
