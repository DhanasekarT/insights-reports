package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ValidateUserPermissionServiceImpl implements ValidateUserPermissionService {

	@Autowired
	private BusinessLogicService businessLogicService;
	
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
					for (String value : fieldsDetails.getValue().split(",")) {
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
		userFiltersValue.put("orgFilters", orgValue);
		userFiltersValue.put("userFilters", userValue);

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
					return businessLogicService.changeDataSourceUserToAnonymousUser(requestParamsDTO);
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
		RequestParamsFilterFieldsDTO systemContentFields = new RequestParamsFilterFieldsDTO("in", APIConstants.CONTENTORGUID, userOrgUId, "String", "selector");
		userFilters.add(systemContentFields);
		systeFilterDetails.setLogicalOperatorPrefix("OR");
		RequestParamsFilterFieldsDTO systemUserFields = new RequestParamsFilterFieldsDTO("in", APIConstants.USERORGID, userOrgUId, "String", "selector");
		userFilters.add(systemUserFields);
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public List<RequestParamsFilterDetailDTO> addSystemContentOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {
	
		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemContentFields = new RequestParamsFilterFieldsDTO("in", APIConstants.CONTENTORGUID, userOrgUId, "String", "selector");
		userFilters.add(systemContentFields);
		systeFilterDetails.setLogicalOperatorPrefix("OR");
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public List<RequestParamsFilterDetailDTO> addSystemUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {

		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemUserFields = new RequestParamsFilterFieldsDTO("in", APIConstants.USERORGID, userOrgUId, "String", "selector");
		userFilters.add(systemUserFields);
		systeFilterDetails.setLogicalOperatorPrefix("OR");
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public String getRoleBasedParty(String traceId,Map<String, Set<String>> partyPermissions, String permission) {
		StringBuilder allowedOrg = new StringBuilder();
		for (Map.Entry<String, Set<String>> entry : partyPermissions.entrySet()) {
			if (entry.getValue().contains(permission)) {
				allowedOrg.append(allowedOrg.length() == 0 ? "" : ",");
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

}
