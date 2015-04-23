package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.ResponseParamDTO;

public interface BusinessLogicService {

	List<Map<String,Object>> customizeJSON(String traceId,String[] groupBy,String resultData,Map<String,String> metrics, Map<String,Boolean> validatedData,ResponseParamDTO<Map<String,Object>> responseParamDTO,int limit);

	List<Map<String,Object>> leftJoin(List<Map<String,Object>> parent,List<Map<String,Object>> child,Set<String> keys);

	List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);

	Map<String,Object> fetchFilters(String index,List<Map<String,Object>> dataList);
	
	RequestParamsDTO changeDataSourceUserToAnonymousUser(RequestParamsDTO requestParamsDTO);
	
	List<Map<String,Object>> getSource(String traceId,String result);
	
	List<Map<String,Object>> formatAggregateKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException;
		
	List<Map<String,Object>> customPagination(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData);
	
	List<Map<String,Object>> aggregatePaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData);
		
	List<Map<String,Object>> customSort(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData);

	List<Map<String,Object>> getRecords(String traceId,String indices,ResponseParamDTO<Map<String,Object>> responseParamDTO,String data, String dataKey)throws Exception;
	
	List<Map<String,Object>> getMultiGetRecords(String traceId,String[] indices,Map<String,Map<String,String>> comparekey,String data,Map<Integer,String> errorRecord,String dataKey);

	String esFields(String index, String fields);
}
