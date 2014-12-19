package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;

public interface ValidateUserPermissionService {

	public Map<String, Object> getAllowedFilters(String gooruUId);
	
	public Map<String, Object> getUserFiltersAndValues(List<RequestParamsFilterDetailDTO> filters);
	
	public Map<Integer, String> checkIfFieldValueMatch(Map<String, Object> allowedFilters, Map<String, Object> userFilters, Map<Integer, String> errorMap);
	
	public RequestParamsDTO userPreValidation(RequestParamsDTO requestParamsDTO, Set<String> userFilterUserValues, Map<String, Set<String>> partyPermissions, Map<Integer, String> errorMap);
	
	public RequestParamsDTO validateOrganization(RequestParamsDTO requestParamsDTO, Map<String, Set<String>> partyPermissions, Map<Integer, String> errorMap, Set<String> userFilterOrgValues);
	
	public List<RequestParamsFilterDetailDTO> addSystemContentUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId);
	
	public List<RequestParamsFilterDetailDTO> addSystemContentOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId);
	
	public List<RequestParamsFilterDetailDTO> addSystemUserOrgFilter(List<RequestParamsFilterDetailDTO> userFilter, String userOrgUId);
	
	public String getAllowedParties(RequestParamsDTO requestParamsDTO,Map<String, Set<String>> partyPermissions);
}
