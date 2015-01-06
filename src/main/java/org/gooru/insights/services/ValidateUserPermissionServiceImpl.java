package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ErrorCodes;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.springframework.stereotype.Component;

@Component
public class ValidateUserPermissionServiceImpl implements ValidateUserPermissionService, APIConstants, ErrorCodes {

	public Map<String, Object> getUserFilters(String gooruUId) {

		final Map<String, Object> allowedFilters = new HashMap<String, Object>();

		allowedFilters.put(CREATORUID, gooruUId);
		allowedFilters.put(GOORUUID, gooruUId);
		allowedFilters.put(CREATOR_UID, gooruUId);
		allowedFilters.put(GOORU_UID, gooruUId);
		allowedFilters.put(USERUID, gooruUId);

		return allowedFilters;
	}

	public Map<String, Object> getUserFiltersAndValues(List<RequestParamsFilterDetailDTO> filters) {
		Map<String, Object> userFiltersValue = null;
		Set<String> userValue = null;
		Set<String> orgValue = null;
		if (filters != null) {
			userFiltersValue = new HashMap<String, Object>();
			userValue = new HashSet<String>();
			orgValue = new HashSet<String>();
			for (RequestParamsFilterDetailDTO fieldData : filters) {
				for (RequestParamsFilterFieldsDTO fieldsDetails : fieldData.getFields()) {
					Set<Object> values = new HashSet<Object>();
					for (String value : fieldsDetails.getValue().split(",")) {
						values.add(value);
						userFiltersValue.put(fieldsDetails.getFieldName(), values);

						if (fieldsDetails.getFieldName().equalsIgnoreCase(CONTENTORGUID) || fieldsDetails.getFieldName().equalsIgnoreCase(CONTENT_ORG_UID) || fieldsDetails.getFieldName().equalsIgnoreCase(USERORGID) || fieldsDetails.getFieldName().equalsIgnoreCase(USER_ORG_UID)) {
							orgValue.add(value);
						}
						if (fieldsDetails.getFieldName().equalsIgnoreCase(CREATORUID) || fieldsDetails.getFieldName().equalsIgnoreCase(CREATOR_UID) || fieldsDetails.getFieldName().equalsIgnoreCase(GOORUUID) || fieldsDetails.getFieldName().equalsIgnoreCase(GOORU_UID)
								|| fieldsDetails.getFieldName().equalsIgnoreCase(USERUID)) {
							userValue.add(value);
						}
					}
				}
			}
			userFiltersValue.put("orgFilters", orgValue);
			userFiltersValue.put("userFilters", userValue);
		}

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
		if(notAllow){
			errorMap.put(403, E1009);
		}
		if(isFilterAvailable && !notAllow){
			errorMap.put(200, E1012);
		}
		return errorMap;
	}

	public RequestParamsDTO userPreValidation(RequestParamsDTO requestParamsDTO, Set<String> userFilterUserValues, Map<String, Set<String>> partyPermissions, Map<Integer, String> errorMap) {
		for (String userFilterUserValue : userFilterUserValues) {
			if (requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES)) {
				if (!(partyPermissions.containsKey(userFilterUserValue) && partyPermissions.get(userFilterUserValue).contains(AP_PARTY_OWN_CONTENT_USAGE))) {
					return requestParamsDTO;
				}
			}
			if ((requestParamsDTO.getDataSource().matches(CONTENTDATASOURCES) && !requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES))) {
				if (!(partyPermissions.containsKey(userFilterUserValue) && partyPermissions.get(userFilterUserValue).contains(AP_PARTY_OWN_CONTENT_USAGE))) {
					return requestParamsDTO;
				}
			}
		}
		errorMap.clear();
		return requestParamsDTO;
	}

	public RequestParamsDTO validateOrganization(RequestParamsDTO requestParamsDTO, Map<String, Set<String>> partyPermissions, Map<Integer, String> errorMap, Set<String> userFilterOrgValues) {
		for (String userFilterOrgValue : userFilterOrgValues) {
			if (requestParamsDTO.getDataSource().matches(USERDATASOURCES)) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(AP_PARTY_PII))) {
					errorMap.put(403, E1003);
					return requestParamsDTO;
				}
			} else if ((requestParamsDTO.getDataSource().matches(CONTENTDATASOURCES) || requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES)) && !StringUtils.isBlank(requestParamsDTO.getGroupBy()) && requestParamsDTO.getGroupBy().matches(USERFILTERPARAM)) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(AP_PARTY_PII))) {
					errorMap.put(403, E1003);
					return requestParamsDTO;
				}
			} else if (requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy())) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(AP_PARTY_ACTIVITY_RAW))) {
					errorMap.put(403, E1004);
					return requestParamsDTO;
				}
			} else if (requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES) && !StringUtils.isBlank(requestParamsDTO.getGroupBy())) {
				if (!(partyPermissions.containsKey(userFilterOrgValue) && partyPermissions.get(userFilterOrgValue).contains(AP_PARTY_ACTIVITY))) {
					errorMap.put(403, E1005);
					return requestParamsDTO;
				}
			}
		}
		return requestParamsDTO;
	}

	public List<RequestParamsFilterDetailDTO> addSystemContentUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {
		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemContentFields = new RequestParamsFilterFieldsDTO("in", CONTENTORGUID, userOrgUId, "String", "selector");
		userFilters.add(systemContentFields);
		systeFilterDetails.setLogicalOperatorPrefix("OR");
		RequestParamsFilterFieldsDTO systemUserFields = new RequestParamsFilterFieldsDTO("in", USERORGID, userOrgUId, "String", "selector");
		userFilters.add(systemUserFields);
		systeFilterDetails.setFields(userFilters);
		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public List<RequestParamsFilterDetailDTO> addSystemContentOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {
		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemContentFields = new RequestParamsFilterFieldsDTO("in", CONTENTORGUID, userOrgUId, "String", "selector");
		userFilters.add(systemContentFields);
		systeFilterDetails.setLogicalOperatorPrefix("OR");
		systeFilterDetails.setFields(userFilters);

		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public List<RequestParamsFilterDetailDTO> addSystemUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId) {

		RequestParamsFilterDetailDTO systeFilterDetails = new RequestParamsFilterDetailDTO();
		List<RequestParamsFilterFieldsDTO> userFilters = new ArrayList<RequestParamsFilterFieldsDTO>();
		RequestParamsFilterFieldsDTO systemUserFields = new RequestParamsFilterFieldsDTO("in", USERORGID, userOrgUId, "String", "selector");
		userFilters.add(systemUserFields);
		systeFilterDetails.setLogicalOperatorPrefix("OR");
		systeFilterDetails.setFields(userFilters);

		userFilter.add(systeFilterDetails);
		return userFilter;
	}

	public String getRoleBasedParty(Map<String, Set<String>> partyPermissions, String permission) {
		StringBuilder allowedOrg = new StringBuilder();
		for (Map.Entry<String, Set<String>> entry : partyPermissions.entrySet()) {
			if (entry.getValue().contains(permission)) {
				allowedOrg.append(allowedOrg.length() == 0 ? "" : ",");
				allowedOrg.append(entry.getKey().toString());
			}
		}
		if (allowedOrg.length() == 0 && !permission.matches(RESTRICTEDPERMISSION)) {
			allowedOrg.append(DEFAULTORGUID);
		}

		System.out.print("allowedOrg : " + allowedOrg.toString());
		return allowedOrg.toString();
	}

	public String getAllowedParties(RequestParamsDTO requestParamsDTO, Map<String, Set<String>> partyPermissions) {
		String allowedParty = null;
		if ((requestParamsDTO.getDataSource().matches(USERDATASOURCES)) || (requestParamsDTO.getDataSource().matches(CONTENTDATASOURCES) || requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES)) && !StringUtils.isBlank(requestParamsDTO.getGroupBy())
				&& requestParamsDTO.getGroupBy().matches(USERFILTERPARAM)) {
			allowedParty = AP_PARTY_PII;
		} else if (requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy())) {
			allowedParty = AP_PARTY_ACTIVITY_RAW;
		} else if (requestParamsDTO.getDataSource().matches(ACTIVITYDATASOURCES)) {
			allowedParty = AP_PARTY_ACTIVITY;
		} else if (requestParamsDTO.getDataSource().matches(CONTENTDATASOURCES)) {
			allowedParty = AP_PARTY_OWN_CONTENT_USAGE;
		}
		return getRoleBasedParty(partyPermissions, allowedParty);
	}

}
