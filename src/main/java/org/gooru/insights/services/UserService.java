package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;

 interface UserService {

	 Map<String, Object> getUserFilters(String gooruUId);
	
	 Map<String, Object> getUserFiltersAndValues(List<RequestParamsFilterDetailDTO> filters);
	
	 Map<Integer, String> checkIfFieldValueMatch(Map<String, Object> allowedFilters, Map<String, Object> userFilters, Map<Integer, String> errorMap);
	
	 RequestParamsDTO userPreValidation(RequestParamsDTO requestParamsDTO, Set<String> userFilterUserValues, Map<String, Set<String>> partyPermissions);
	
	 RequestParamsDTO validateOrganization(RequestParamsDTO requestParamsDTO, Map<String, Set<String>> partyPermissions, Map<Integer, String> errorMap, Set<String> userFilterOrgValues);
	
	 List<RequestParamsFilterDetailDTO> addSystemContentUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId);
	
	 List<RequestParamsFilterDetailDTO> addSystemContentOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId);
	
	 List<RequestParamsFilterDetailDTO> addSystemUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId);
	
	 String getAllowedParties(String traceId,RequestParamsDTO requestParamsDTO,Map<String, Set<String>> partyPermissions);
	 
	 String getRoleBasedParty(String traceId,Map<String, Set<String>> partyPermissions, String permission);

	 RequestParamsDTO validateUserRole(String traceId,RequestParamsDTO requestParamsDTO,Map<String,Object> userMap);
 }
