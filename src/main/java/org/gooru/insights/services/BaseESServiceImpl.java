package org.gooru.insights.services;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

@Service
public class BaseESServiceImpl implements BaseESService,APIConstants,ESConstants {

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	UpdatedService updatedService;

	RequestParamsSortDTO requestParamsSortDTO;

	RequestParamsFilterDetailDTO requestParamsFiltersDetailDTO;

	public Map<String, Object> record(String index, String type, String id) {
		GetResponse response = getClient().prepareGet(index, type, id)
				.execute().actionGet();
		return response.getSource();
	}
	public long recordCount(String[] indices, String[] types,
			QueryBuilder query, String id) {
		CountRequestBuilder response = getClient().prepareCount(indices);
		if (query != null) {
			response.setQuery(query);
		}
		if (types != null && types.length >= 0) {
			response.setTypes(types);
		}
		return response.execute().actionGet().getCount();
	}

	public JSONArray itemSearch(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord) {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Set<Object>> filterMap = new HashMap<String,Set<Object>>();
		boolean multiGet = false; 
		dataList = searchData(requestParamsDTO,new String[]{ indices[0]},new String[]{ indexTypes.get(indices[0])},validatedData,dataMap,errorRecord,multiGet,filterMap);
		System.out.println(" result data : "+dataList);
		if(dataList.isEmpty())
		return new JSONArray();			
		filterMap = fetchFilters(indices[0], dataList);
		System.out.println("filter Map: "+filterMap);
		for(int i=1;i<indices.length;i++){
			Set<String> usedFilter = new HashSet<String>();
			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,new String[]{ indices[i]}, new String[]{ indexTypes.get(indices[i])}, validatedData,filterMap,errorRecord,dataList.size(),usedFilter);
			
			System.out.println("result : "+resultList);
			dataList = leftJoin(dataList, resultList,usedFilter);
		}
		System.out.println("combined "+ dataList);
		if(validatedData.get(hasdata.HAS_GROUPBY.check())){
		try {
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			dataList = formatAggregateKeyValueJson(dataList, groupBy[groupBy.length-1]);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		dataList = aggregatePaginate(requestParamsDTO.getPagination(), dataList, validatedData, dataMap);		
		}
	return convertJSONArray(dataList);
	}
	
	public List<Map<String,Object>> multiGet(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Set<Object>> filterMap,Map<Integer,String> errorRecord,Integer limit,Set<String> usedFilter) {
		
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);

		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		String result ="[{}]";
		String dataKey=esSources.SOURCE.esSource();
		String fields = esFields(indices[0],requestParamsDTO.getFields());
		
		if(fields.contains("code_id") || fields.contains("label")){
		fields = fields+",depth";	
		}
		
		if(validatedData.get(hasdata.HAS_FEILDS.check()))
			dataKey=esSources.FIELDS.esSource();
		
		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}

		if(!filterMap.isEmpty())
		searchRequestBuilder.setPostFilter(updatedService.customFilter(indices[0],filterMap,usedFilter));


		searchRequestBuilder.setPreference("_primaries");

		searchRequestBuilder.setSize(limit);
		
		try{
			System.out.println("mutiget query "+searchRequestBuilder);
		result =  searchRequestBuilder.execute().actionGet().toString();
		System.out.println("mutiget data "+result);
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		
		Map<String,Map<String,String>> compareMap = new HashMap<String, Map<String,String>>();
		
		resultList = getRecords(indices,result, errorRecord, dataKey);
		
		return resultList;
	}
	
	public List<Map<String,Object>> leftJoin(List<Map<String,Object>> parent,List<Map<String,Object>> child,Set<String> keys){
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> parentEntry : parent) {
			boolean occured = false;
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> childEntry : child) {
				boolean validated = false;
				for(String key : keys){
					
				if (childEntry.containsKey(key) && parentEntry.containsKey(key)) {
					if (childEntry.get(key).equals(parentEntry.get(key))) {
					}else{
						
						validated = true;
					}
				}else{
					validated = true;
				}
				}
				if(!validated){
						occured = true;
						appended.putAll(childEntry);
						appended.putAll(parentEntry);
						break;
				}
			if (!occured) {
				appended.putAll(parentEntry);
			}
			}

			resultList.add(appended);
		}
		return resultList;
	}
	
	public List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> parentEntry : parent) {
			boolean occured = false;
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> childEntry : child) {
				if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
					if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
						occured = true;
						appended.putAll(childEntry);
						appended.putAll(parentEntry);
						break;
					}
				}
			}
			if (!occured) {
				appended.putAll(parentEntry);
			}

			resultList.add(appended);
		}
		return resultList;
	}
	
	public List<Map<String,Object>> searchData(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord,Boolean multiGet,Map<String,Set<Object>> filterMap) {
		
		Map<String,String> metricsName = new HashMap<String,String>();
		boolean hasAggregate = false;
		String result ="[{}]";
		String fields = esFields(indices[0],requestParamsDTO.getFields());
		String dataKey=esSources.SOURCE.esSource();

		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);


		if(validatedData.get(hasdata.HAS_FEILDS.check()))
			dataKey=esSources.FIELDS.esSource();
		
		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}
		
		if (validatedData.get(hasdata.HAS_GRANULARITY.check())) {
			updatedService.granularityAggregate(indices[0],requestParamsDTO, searchRequestBuilder,metricsName,validatedData);
			hasAggregate = true;
			} 
		
		if (!validatedData.get(hasdata.HAS_GRANULARITY.check()) && validatedData.get(hasdata.HAS_GROUPBY.check())) {
			updatedService.aggregate(indices[0],requestParamsDTO, searchRequestBuilder,metricsName,validatedData);
			hasAggregate = true;
		}
		
		if(!hasAggregate){
				// Add filter in Query
				if(validatedData.get(hasdata.HAS_FILTER.check()))
				searchRequestBuilder.setPostFilter(updatedService.includeFilter(indices[0],requestParamsDTO.getFilter()));
		}
		
		
		/*we comment ES level sorting since it does not support metrics sort
		 * if(validatedData.get(hasdata.HAS_SORTBY.check()) && !hasAggregate)
		sortData(requestParamsDTO.getPagination().getOrder(),searchRequestBuilder,validatedData);
		*/
		
		 searchRequestBuilder.setPreference("_primaries");

		 //currently its not working in current ES version 1.2.2,its shows record count is 1 * no of shades = total Records
System.out.println(" pagination status "+validatedData);
		 if(validatedData.get(hasdata.HAS_PAGINATION.check()))
		paginate(searchRequestBuilder, requestParamsDTO.getPagination(), validatedData);
		
		System.out.println("query \n"+searchRequestBuilder);
		
		try{
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		
		if(hasAggregate){
		try {
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			List<Map<String,Object>> data = updatedService.buildJSON(groupBy, result, metricsName, validatedData.get(hasdata.HAS_FILTER.check()));
//			data = customPaginate(requestParamsDTO.getPagination(), data, validatedData,dataMap);
			data = aggregateSortBy(requestParamsDTO.getPagination(), data, validatedData);
//			data = formatAggregateKeyValueJson(data, groupBy[groupBy.length-1]);
//			data = aggregatePaginate(requestParamsDTO.getPagination(), data, validatedData, dataMap);
			return data;
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
		
		List<Map<String,Object>> data = getRecords(indices,result,errorRecord,dataKey);
		data = customPaginate(requestParamsDTO.getPagination(), data, validatedData, dataMap);
		
		if(multiGet){
			
		}
		return data;

	}
	
	public Map<String,Set<Object>> fetchFilters(String index,List<Map<String,Object>> dataList){
		System.out.println("index:"+index);
		Map<String,String> filterFields = new HashMap<String, String>();
		Map<String,Set<Object>> filters = new HashMap<String, Set<Object>>();
		if(baseConnectionService.getFieldsJoinCache().containsKey(index)){
			filterFields = baseConnectionService.getFieldsJoinCache().get(index);
		}
			for(Map<String,Object> dataMap : dataList){
				Set<String> keySets = filterFields.keySet();
				for(String keySet : keySets){
				for(String key : keySet.split(",")){
					if(dataMap.containsKey(key)){
						if(!filters.isEmpty() && filters.containsKey(key)){
							Set<Object> filterValue = filters.get(key);
							filterValue.add(dataMap.get(key));
							filters.put(key, filterValue);
						}else{
							Set<Object> filterValue = new HashSet<Object>();
							filterValue.add(dataMap.get(key));
							filters.put(key, filterValue);
						}
					}
				}
				}
			}
		
			return filters;
	}
	
	public JSONArray convertJSONArray(List<Map<String,Object>> data){
		try {
		Gson gson = new Gson();
		Type listType = new TypeToken<List<Map<String,Object>>>(){}.getType();
		JsonElement jsonArray = gson.toJsonTree(data, listType);
			return new JSONArray(jsonArray.toString());
		} catch (JSONException e) {
			e.printStackTrace();
			return new JSONArray();
		}
	}
	
	public List<Map<String,Object>> formatAggregateKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException{
		
//		JSONObject json = new JSONObject();
		Map<String,List<Map<String,Object>>> resultMap = new LinkedHashMap<String, List<Map<String,Object>>>();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		Gson gson = new Gson();
		for(Map<String,Object> map : dataMap){
			if(map.containsKey(key)){
				List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
				String jsonKey = map.get(key).toString();
				map.remove(key);
				if(resultMap.containsKey(jsonKey)){
					tempList.addAll(resultMap.get(jsonKey));
				}
					tempList.add(map);
				resultMap.put(jsonKey, tempList);
//					json.accumulate(jsonKey, map);
			}
		}
//		resultMap = gson.fromJson(json.toString(),resultMap.getClass());
	
		/*	Map<String,Object> Treedata = new TreeMap<String, Object>(resultMap);
		resultList.add(Treedata);
		for(Map.Entry<String, Object> entry : Treedata.entrySet()){
			JSONObject resultJson = new JSONObject();
			resultJson.put(entry.getKey(), entry.getValue());
			jsonArray.put(resultJson);
		}*/
		for(Entry<String, List<Map<String, Object>>> entry : resultMap.entrySet()){
			Map<String,Object> tempMap = new HashMap<String, Object>();
			tempMap.put(entry.getKey(), entry.getValue());
			resultList.add(tempMap);
		}
		return resultList;
	}
	
	public JSONArray buildAggregateJSON(List<Map<String,Object>> resultList) throws JSONException{
		Gson gson = new Gson();
		String resultArray = gson.toJson(resultList,resultList.getClass());
		return new JSONArray(resultArray);
	}
	
	public List<Map<String,Object>> customPaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap){
		int dataSize = data.size();
		List<Map<String,Object>> customizedData = new ArrayList<Map<String,Object>>();
		if(baseAPIService.checkNull(requestParamsPaginationDTO)){
			if(validatedData.get(hasdata.HAS_SORTBY.check())){
				List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
				for(RequestParamsSortDTO sortData : orderDatas){
					baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
				}
			}
			int offset = validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() : 0; 
			int limit = validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() : 10; 
			
			if(limit < dataSize && offset < dataSize){
				customizedData = data.subList(offset, limit);
			}else if(limit >= dataSize &&  offset < dataSize){
				customizedData = data.subList(offset, dataSize);
			}else if(limit < dataSize &&  offset >= dataSize){
				customizedData = data.subList(0,limit);
			}else if(limit >= dataSize &&  offset >= dataSize){
				customizedData = data.subList(0,dataSize);
			}
		}else{
			customizedData = data;
		}
		returnMap.put("totalRows",customizedData.size());
		return customizedData;
	}
	
	public List<Map<String,Object>> aggregatePaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap){
		int dataSize = data.size();
		List<Map<String,Object>> customizedData = new ArrayList<Map<String,Object>>();
		if(baseAPIService.checkNull(requestParamsPaginationDTO)){
			int offset = validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() : 0; 
			int limit = validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() : 10; 
			
			if(limit < dataSize && offset < dataSize){
				customizedData = data.subList(offset, limit);
			}else if(limit >= dataSize &&  offset < dataSize){
				customizedData = data.subList(offset, dataSize);
			}else if(limit < dataSize &&  offset >= dataSize){
				customizedData = data.subList(0,limit);
			}else if(limit >= dataSize &&  offset >= dataSize){
				customizedData = data.subList(0,dataSize);
			}
		}else{
			customizedData = data;
		}
		returnMap.put("totalRows",customizedData.size());
		return customizedData;
	}
	
	public List<Map<String,Object>> aggregateSortBy(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData){
		List<Map<String,Object>> customizedData = new ArrayList<Map<String,Object>>();
		if(baseAPIService.checkNull(requestParamsPaginationDTO)){
			if(validatedData.get(hasdata.HAS_SORTBY.check())){
				List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
				for(RequestParamsSortDTO sortData : orderDatas){
					baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
				}
			}
			
		}	
		customizedData = data;
		
		return customizedData;
	}
	public void sortData(String[] indices,List<RequestParamsSortDTO> requestParamsSortDTO,SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validatedData){
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(hasdata.HAS_SORTBY.check()))
				searchRequestBuilder.addSort(esFields(indices[0],sortData.getSortBy()), (baseAPIService.checkNull(sortData.getSortOrder()) && sortData.getSortOrder().equalsIgnoreCase("DESC")) ? SortOrder.DESC : SortOrder.ASC);
		}
	}

	public void paginate(SearchRequestBuilder searchRequestBuilder,RequestParamsPaginationDTO requestParamsPaginationDTO,Map<String,Boolean> validatedData) {
		searchRequestBuilder.setFrom(validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset().intValue() : 0);
		searchRequestBuilder.setSize(validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit().intValue() : 10);
	}

	
	public List<Map<String,Object>> subSearch(RequestParamsDTO requestParamsDTOs,String[] indices,String fields,Map<String,Set<Object>> filtersData){
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		
		if(baseAPIService.checkNull(fields)){
			for(String field : fields.split(",")){
		searchRequestBuilder.addField(field);
			}
		}
		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
		
		for(Map.Entry<String,Set<Object>> entry : filtersData.entrySet()){
			boolFilter.must(FilterBuilders.inFilter(entry.getKey(),baseAPIService.convertSettoArray(entry.getValue())));
		}
		updatedService.includeFilter(indices[0],requestParamsDTOs.getFilter());
		searchRequestBuilder.setPostFilter(boolFilter);
	System.out.println(" sub query \n"+searchRequestBuilder);
	
		String resultSet = searchRequestBuilder.execute().actionGet().toString();
		return getData(fields, resultSet);
	}
	
	public List<Map<String,Object>> getData(String fields,String jsonObject){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try{
			JSONObject json = new JSONObject(jsonObject);
			json = new JSONObject(json.get("hits").toString());
		JSONArray hitsArray = new JSONArray(json.get("hits").toString());
		
		for(int i=0;i<hitsArray.length();i++){
			Map<String,Object> resultMap = new HashMap<String,Object>();
		JSONObject getSourceJson = new JSONObject(hitsArray.get(i).toString());
		getSourceJson = new JSONObject(getSourceJson.get("_source").toString());
		if(baseAPIService.checkNull(fields)){
		for(String field : fields.split(",")){
			if(getSourceJson.has(field)){
			resultMap.put(field, getSourceJson.get(field));
			}
		}
		}else{
			Iterator<String> keys = getSourceJson.keys();
			while(keys.hasNext()){
				String key=keys.next();
				resultMap.put(key, getSourceJson.get(key));
			}
		}
		resultList.add(resultMap);
		}
		}catch(Exception e){
			System.out.println(" get Data method failed");
			e.printStackTrace();
		}
		return resultList;
	}
	
	public List<Map<String,Object>> formDataList(Map<Integer,Map<String,Object>> requestMap){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		for(Map.Entry<Integer,Map<String,Object>> entry : requestMap.entrySet()){
			resultList.add(entry.getValue());
		}
		return resultList;
	}
	
	public JSONArray formDataJSONArray(Map<Integer,Map<String,Object>> requestMap){
		JSONArray jsonArray = new JSONArray();
		for(Map.Entry<Integer,Map<String,Object>> entry : requestMap.entrySet()){
			jsonArray.put(entry.getValue());
		}
		return jsonArray;
	}
	
	public List<Map<String,Object>> getSource(String result){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			Gson gson = new Gson();
			Type mapType = new TypeToken<Map<String,Object>>(){}.getType();
			JSONObject mainJson = new JSONObject(result);
			mainJson = new JSONObject(mainJson.get("hits").toString());
			JSONArray jsonArray = new JSONArray(mainJson.get("hits").toString());
			for(int i=0;i<jsonArray.length();i++){
				 mainJson = new JSONObject(jsonArray.get(i).toString());
				 Map<String,Object> dataMap = new HashMap<String,Object>();	 
				 dataMap = gson.fromJson(mainJson.getString("_source"),mapType);
				 resultList.add(dataMap);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	public List<Map<String,Object>> formJoinKey(Map<String,Set<Object>> filtersMap){
		List<Map<String,Object>> formedList = new ArrayList<Map<String,Object>>();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		for(Map.Entry<String,Set<Object>> entry : filtersMap.entrySet()){
			for(Object value : entry.getValue()){
				Map<String,Object> formedMap = new HashMap<String,Object>();
			formedMap.put(entry.getKey(), value);
			formedList.add(formedMap);
			}
		}
		if(baseAPIService.checkNull(formedList)){
			if(baseAPIService.checkNull(resultList)){
				List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
				for(Map<String,Object> resultMap : resultList){
					for(Map<String,Object> formedMap : formedList){
						tempList.add(formedMap);
						tempList.add(resultMap);
					}	
				}
				resultList = tempList;
			}else{
				resultList = formedList;
			}
		}
		return resultList;
	}
	public Client getClient() {
		return baseConnectionService.getClient();
	}
	
	
	public List<Map<String,Object>> getRecords(String[] indices,String data,Map<Integer,String> errorRecord,String dataKey){
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get("hits").toString());
			jsonArray = new JSONArray(json.get("hits").toString());
			if(!dataKey.equalsIgnoreCase("fields")){
			for(int i =0;i< jsonArray.length();i++){
				json = new JSONObject(jsonArray.get(i).toString());
				JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
				
				Iterator<String> keys = fieldJson.keys();
				Map<String,Object> resultMap = new HashMap<String, Object>();
				while(keys.hasNext()){
					String key =keys.next(); 
					resultMap.put(apiFields(indices[0],key), fieldJson.get(key));
				}
				resultList.add(resultMap);
			}
			}else{
				for(int i =0;i< jsonArray.length();i++){
					Map<String,Object> resultMap = new HashMap<String, Object>();
				json = new JSONObject(jsonArray.get(i).toString());
				json = new JSONObject(json.get(dataKey).toString());
				 Iterator<String> keys =json.keys();
				 while(keys.hasNext()){
					 String key = keys.next();
					 JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
					if(fieldJsonArray.length() == 1){	 
						resultMap.put(apiFields(indices[0],key),fieldJsonArray.get(0));
					}else{
						resultMap.put(apiFields(indices[0],key),fieldJsonArray);
					}
				 }
				 resultList.add(resultMap);
			}
			}
			return resultList;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	
	public List<Map<String,Object>> getMultiGetRecords(String[] indices,Map<String,Map<String,String>> comparekey,String data,Map<Integer,String> errorRecord,String dataKey){
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get("hits").toString());
			jsonArray = new JSONArray(json.get("hits").toString());
			if(!dataKey.equalsIgnoreCase("fields")){
			for(int i =0;i< jsonArray.length();i++){
				json = new JSONObject(jsonArray.get(i).toString());
				JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
				
				Iterator<String> keys = fieldJson.keys();
				Map<String,Object> resultMap = new HashMap<String, Object>();
				while(keys.hasNext()){
					String key =keys.next(); 
					if(comparekey.containsKey(key)){
						Map<String,String> comparable = new HashMap<String, String>();
						comparable = comparekey.get(key);
						for(Map.Entry<String, String> compare : comparable.entrySet()){
							
							if(compare.getKey().equalsIgnoreCase(fieldJson.get(key).toString()))
									resultMap.put(compare.getValue(), fieldJson.get(key));
						}
					}else{
					resultMap.put(apiFields(indices[0],key), fieldJson.get(key));
					}
				}
				resultList.add(resultMap);
			}
			}else{
				for(int i =0;i< jsonArray.length();i++){
					Map<String,Object> resultMap = new HashMap<String, Object>();
				json = new JSONObject(jsonArray.get(i).toString());
				json = new JSONObject(json.get(dataKey).toString());
				 Iterator<String> keys =json.keys();
				 while(keys.hasNext()){
					 String key = keys.next();
					 JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
					if(fieldJsonArray.length() == 1){	 
						if(comparekey.containsKey(key)){
							Map<String,String> comparable = new HashMap<String, String>();
							comparable = comparekey.get(key);
							for(Map.Entry<String, String> compare : comparable.entrySet()){
								
								if(compare.getKey().equalsIgnoreCase(fieldJsonArray.get(0).toString()))
										resultMap.put(compare.getValue(), fieldJsonArray.getString(0));
							}
						}else{
						resultMap.put(apiFields(indices[0],key),fieldJsonArray.get(0));
						}
					}else{
						if(comparekey.containsKey(key)){
							Map<String,String> comparable = new HashMap<String, String>();
							comparable = comparekey.get(key);
							for(Map.Entry<String, String> compare : comparable.entrySet()){
								
								if(compare.getKey().equalsIgnoreCase(fieldJsonArray.get(0).toString()))
										resultMap.put(compare.getValue(), fieldJsonArray);
							}
						}else{
						resultMap.put(apiFields(indices[0],key),fieldJsonArray);
						}
					}
				 }
				 resultList.add(resultMap);
			}
			}
			return resultList;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	public String esFields(String index,String fields){
		System.out.println("index : "+index+" and fields : "+fields );
		Map<String,String> mappingfields = baseConnectionService.getFields().get(index);
		StringBuffer esFields = new StringBuffer();
		for(String field : fields.split(",")){
			if(esFields.length() > 0){
				esFields.append(",");
			}
			if(mappingfields.containsKey(field)){
				esFields.append(mappingfields.get(field));
			}else{
				esFields.append(field);
			}
		}
		return esFields.toString();
	}
	
	public String apiFields(String index,String fields){
		Map<String,String> mappingfields = baseConnectionService.getFields().get(index);
		Set<String> keys = mappingfields.keySet();
		Map<String,String> apiFields =new HashMap<String, String>();
		for(String key : keys){
			apiFields.put(mappingfields.get(key), key);
		}
		StringBuffer esFields = new StringBuffer();
		for(String field : fields.split(",")){
			if(esFields.length() > 0){
				esFields.append(",");
			}
			if(apiFields.containsKey(field)){
				esFields.append(apiFields.get(field));
			}else{
				esFields.append(field);
			}
		}
		return esFields.toString();
	}
}
