package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.builders.utils.ExcludeNullTransformer;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.constants.ResponseParamDTO;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

import flexjson.JSONSerializer;

@Service
public class ItemServiceImpl implements ItemService {
	
	Logger logger = LoggerFactory.getLogger(ItemServiceImpl.class);

	@Autowired
	BaseAPIService baseAPIService;

	@Autowired
	BaseESService esService;

	@Autowired
	BusinessLogicService businessLogicService;

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseCassandraService baseCassandraService;
	
	JSONSerializer serializer = new JSONSerializer();
	
	/**
	 * This will return simple message as service available
	 */
	public ResponseParamDTO<Map<String,Object>> serverStatus(){
		
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		Map<String,Object> message = new HashMap<String, Object>();
		message.put(MessageHandler.getMessage(APIConstants.STATUS_NAME),MessageHandler.getMessage(APIConstants.TOMCAT_STATUS));
		responseParamDTO.setMessage(message);
		return responseParamDTO;
	}
	
	/**
	 * This will process the API
	 * 
	 */
	public ResponseParamDTO<Map<String,Object>> processApi(String data, Map<String, Object> userMap) throws Exception {

		List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>();
		RequestParamsCoreDTO requestParamsCoreDTO = baseAPIService.buildRequestParamsCoreDTO(data);
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		if (baseAPIService.checkNull(requestParamsCoreDTO.getRequestParamsDTO())) {
			List<RequestParamsDTO> requestParamsDTOs = requestParamsCoreDTO.getRequestParamsDTO();

			String previousAPIKey = null;
			for (RequestParamsDTO api : requestParamsDTOs) {
				if (!baseAPIService.checkNull(api)) {
					continue;
				}
				responseParamDTO = generateQuery(data, userMap);
				if (baseAPIService.checkNull(previousAPIKey)) {
					resultData = businessLogicService.leftJoin(resultData, responseParamDTO.getContent(), previousAPIKey, api.getApiJoinKey());
				}
			}
			if (baseAPIService.checkNull(requestParamsCoreDTO.getCoreKey())) {
				resultData = businessLogicService.formatAggregateKeyValueJson(resultData, requestParamsCoreDTO.getCoreKey());
			}
		}
		responseParamDTO.setContent(resultData);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getPartyReport(HttpServletRequest request,String reportType, Map<String, Object> userMap) throws Exception {
		RequestParamsDTO systemRequestParamsDTO = null;
		boolean isMerged = false;

		Map<String,Object> filtersMap = baseAPIService.getRequestFieldNameValueInMap(request, "f");
		Map<String,Object> paginationMap = baseAPIService.getRequestFieldNameValueInMap(request, "p");
		
		if(filtersMap.isEmpty()){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100, APIConstants.FILTERS));
		}
		
		Column<String> val = baseCassandraService.readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(),APIConstants.DI_REPORTS,reportType);
		
		if(val == null){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E106));
		}
		
		ColumnList<String> columns = baseCassandraService.read(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue());
		
		systemRequestParamsDTO = baseAPIService.buildRequestParameters(columns.getStringValue("query", null));
		for(RequestParamsFilterDetailDTO systemFieldData : systemRequestParamsDTO.getFilter()) {
			for(RequestParamsFilterFieldsDTO systemfieldsDetails : systemFieldData.getFields()) {
				if(filtersMap.containsKey(systemfieldsDetails.getFieldName())){
					isMerged = true;
					String[] values = filtersMap.get(systemfieldsDetails.getFieldName()).toString().split(",");
					systemfieldsDetails.setValue(filtersMap.get(systemfieldsDetails.getFieldName()).toString());
					if(values.length > 1){
						systemfieldsDetails.setOperator("in");
					}
				}
			}
		}
		if(!isMerged){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E107,APIConstants.FILTERS));
		}

		if(!paginationMap.isEmpty()){
			if(paginationMap.containsKey("limit")){
				systemRequestParamsDTO.getPagination().setLimit(Integer.valueOf(""+paginationMap.get("limit")));
			}
			if(paginationMap.containsKey("offset")){
				systemRequestParamsDTO.getPagination().setOffset(Integer.valueOf(""+paginationMap.get("offset")));
			}
			if(paginationMap.containsKey("sortOrder")){
				for(RequestParamsSortDTO requestParamsSortDTO :   systemRequestParamsDTO.getPagination().getOrder()){
					requestParamsSortDTO.setSortOrder(paginationMap.get("sortOrder").toString());
				}
			}
		}
		
		logger.info(APIConstants.OLD_QUERY+columns.getStringValue(APIConstants.QUERY, null));

		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		logger.info(APIConstants.NEW_QUERY+datas);
		
		if(columns.getStringValue(APIConstants.QUERY, null) != null){			
			return generateQuery(datas, userMap);
		}
		return new ResponseParamDTO<Map<String,Object>>();
	}
	
	//valid
	public ResponseParamDTO<Map<String,Object>> generateQuery(String data,Map<String, Object> userMap) throws Exception {
		
		RequestParamsDTO requestParamsDTO = null;
			requestParamsDTO = baseAPIService.buildRequestParameters(data);
		
		Map<String, Boolean> checkPoint = baseAPIService.checkPoint(requestParamsDTO);

		/**
		 * Additional filters are added based on user authentication
		 */
		requestParamsDTO = baseAPIService.validateUserRole(requestParamsDTO, userMap);


		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		ResponseParamDTO<Map<String,Object>> responseParamDTO = esService.generateQuery(requestParamsDTO, indices, checkPoint);
			/**
			 * save data to redis
			 */
			baseAPIService.saveQuery(requestParamsDTO,responseParamDTO,data,userMap);
			return responseParamDTO;
			
	}

	public ResponseParamDTO<Map<String,String>> clearQuery(String id) {
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		Map<String, String> dataMap = new HashMap<String, String>();
		String message = APIConstants.EMPTY;
		if(baseAPIService.checkNull(id)){
		if(baseAPIService.clearQuery(id)){
			message = MessageHandler.getMessage(APIConstants.STATUS, new String[]{APIConstants.QUERY,APIConstants.DELETED});
		}else{
			message = MessageHandler.getMessage(APIConstants.STATUS, new String[]{APIConstants.QUERY,APIConstants.NOT_FOUND});
			
		}
		}else{
			message = MessageHandler.getMessage(APIConstants.STATUS, new String[]{APIConstants.QUERYS,APIConstants.DELETED});
			
		dataMap.put(MessageHandler.getMessage(APIConstants.STATUS_NAME),message);
		}
		responseParamDTO.setMessage(dataMap);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String,Object>> getQuery(String id,Map<String,Object> dataMap) {

		String prefix = APIConstants.EMPTY;
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		 if(dataMap.containsKey(APIConstants.GOORUUID) && dataMap.get(APIConstants.GOORUUID) != null){
			 prefix = dataMap.get(APIConstants.GOORUUID).toString()+APIConstants.SEPARATOR;
		 }
		String result = baseAPIService.getQuery(prefix,id);
		baseAPIService.deserialize(result, responseParamDTO.getClass());
		return responseParamDTO;
	}

	
	
	public ResponseParamDTO<Map<String,Object>> getCacheData(String id,Map<String,Object> userMap) {


		 String prefix = APIConstants.EMPTY;
		 if(userMap.containsKey(APIConstants.GOORUUID) && userMap.get(APIConstants.GOORUUID) != null){
			 prefix = userMap.get(APIConstants.GOORUUID).toString()+APIConstants.SEPARATOR;
		 }
		 ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		 List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			if (id != null && !id.isEmpty()) {
				for (String requestId : id.split(APIConstants.COMMA)) {
					Map<String,Object> dataMap = new HashMap<String, Object>();
					do {
						dataMap.put(requestId, baseAPIService.getKey(prefix+requestId) != null ? baseAPIService.deserialize(baseAPIService.getKey(prefix+requestId),ResponseParamDTO.class).getContent() : new ArrayList<Map<String,Object>>());
						requestId = baseAPIService.getKey(prefix+requestId);
						resultList.add(dataMap);
					} while (baseAPIService.hasKey(prefix+requestId));
				}
			} else {
				Set<String> keyIds = baseAPIService.getKeys();
				Set<String> customizedKey = new HashSet<String>();
				for (String keyId : keyIds) {
					if (keyId.contains(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + APIConstants.CACHE_PREFIX_ID+prefix)) {
					customizedKey.add(keyId.replaceAll(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR+prefix, ""));
					}else{
					customizedKey.add(keyId.replaceAll(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR+prefix, ""));
					}
				}
				for (String requestId : customizedKey) {
					Map<String,Object> dataMap = new HashMap<String, Object>();
					dataMap.put(requestId, baseAPIService.getKey(prefix+requestId) != null ? baseAPIService.deserialize(baseAPIService.getKey(prefix+requestId),ResponseParamDTO.class).getContent() : new ArrayList<Map<String,Object>>());
					requestId = baseAPIService.getKey(prefix+requestId);
					resultList.add(dataMap);
				}
			}
		} catch (Exception e) {
			logger.error(ErrorConstants.REDIS_MESSAGE.replace(ErrorConstants.REPLACER, ErrorConstants.GET));
		}
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<Integer,String>> manageReports(String action,String reportName,String data){
		
		 ResponseParamDTO<Map<Integer,String>> responseParamDTO = new ResponseParamDTO<Map<Integer,String>>();
		 Map<Integer,String> resultMap = new HashMap<Integer, String>();
		if(action.equalsIgnoreCase("add")){
			Column<String> val = baseCassandraService.readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS,reportName);
			
			if(val == null || (val !=null && StringUtils.isBlank(val.getStringValue()))){
				try {
					RequestParamsDTO requestParamsDTO = baseAPIService.buildRequestParameters(data);
				} catch (Exception e) {
					throw new BadRequestException(ErrorConstants.E102);
				}	
				
				UUID reportId = UUID.randomUUID();
	
				baseCassandraService.saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS, reportName, reportId.toString());
				baseCassandraService.saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), reportId.toString(), APIConstants.QUERY, data);

				resultMap.put(200,ErrorConstants.SUCCESSFULLY_ADDED);
			}else{
				throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E105));
			}
		}else if(action.equalsIgnoreCase("update")){
			Column<String> val = baseCassandraService.readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS,reportName);
			
			if(val !=null && !StringUtils.isBlank(val.getStringValue())){
				try {
					RequestParamsDTO requestParamsDTO = baseAPIService.buildRequestParameters(data);
				} catch (Exception e) {
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E102));
					
//					errorMap.put(400,E1014);
//					return errorMap;
				}	
				
				baseCassandraService.saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue(), "query", data);
				
				resultMap.put(200,ErrorConstants.SUCCESSFULLY_ADDED);
			}else{
				throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E105));
			}
		}
		responseParamDTO.setMessage(resultMap);		
		return responseParamDTO;	
	}

	public Map<String, Object> getUserObject(String sessionToken, Map<Integer, String> errorMap) {
		return baseConnectionService.getUserObject(sessionToken, errorMap);
	}

	public Map<String, Object> getUserObjectData(String sessionToken) {
		return baseConnectionService.getUserObjectData(sessionToken);
	}

	public ResponseParamDTO<Map<String,String>> insertKey(String data){
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		Map<String,String> resultData = new HashMap<String, String>();
		if(baseAPIService.insertKey(data)){
			resultData.put(MessageHandler.getMessage(APIConstants.STATUS_NAME), MessageHandler.getMessage(APIConstants.STATUS,new String[]{ APIConstants.QUERY,MessageHandler.getMessage(APIConstants.INSERTED)}));
		}else{
			resultData.put(MessageHandler.getMessage(APIConstants.STATUS_NAME), MessageHandler.getMessage(APIConstants.STATUS,new String[]{ APIConstants.QUERY,MessageHandler.getMessage(APIConstants.FAILED)}));
		}
		responseParamDTO.setMessage(resultData);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> clearDataCache() {
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		Map<String,Object> dataMap = new HashMap<String, Object>();
		baseConnectionService.clearDataCache();
		dataMap.put(MessageHandler.getMessage(APIConstants.STATUS_NAME), MessageHandler.getMessage(APIConstants.CACHE_CLEAR).replace(ErrorConstants.REPLACER, MessageHandler.getMessage(APIConstants.DATA)));	
		responseParamDTO.setMessage(dataMap);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String,Object>> clearConnectionCache() {
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		Map<String,Object> dataMap = new HashMap<String,Object>();
		baseConnectionService.clearConnectionCache();
		dataMap.put(MessageHandler.getMessage(APIConstants.STATUS_NAME), MessageHandler.getMessage(APIConstants.CACHE_CLEAR).replace(ErrorConstants.REPLACER, MessageHandler.getMessage(APIConstants.CONNECTION)));	
		responseParamDTO.setMessage(dataMap);
		return responseParamDTO;
	}

	
}
