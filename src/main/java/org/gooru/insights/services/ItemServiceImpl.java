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
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ResponseParamDTO;
import org.restlet.engine.http.connector.BaseServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

import flexjson.JSONSerializer;

@Service
public class ItemServiceImpl implements ItemService {
	
	private static final Logger logger = LoggerFactory.getLogger(ItemServiceImpl.class);

	@Autowired
	private BaseAPIService baseAPIService;

	@Autowired
	private BaseESService esService;

	@Autowired
	private RedisService redisService;
	
	@Autowired
	private BusinessLogicService businessLogicService;

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private BaseCassandraService baseCassandraService;
	
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
	 * This will process the multiple API
	 * @param data is the requested 
	 * @param sessionToken is user access token
	 */
	public ResponseParamDTO<Map<String,Object>> processApi(String data, String sessionToken) throws Exception {

		Map<String,Object> userMap = getUserObjectData(sessionToken); 
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
				responseParamDTO = generateQuery(data,null, userMap);
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
	
	/**
	 * 
	 */
	public ResponseParamDTO<Map<String,Object>> getPartyReport(HttpServletRequest request,String reportType,String sessionToken) throws Exception {
		RequestParamsDTO systemRequestParamsDTO = null;
		boolean isMerged = false;
		JSONSerializer serializer = new JSONSerializer();

		Map<String, Object> userMap = getUserObjectData(sessionToken);
		
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
			return generateQuery(datas, null, userMap);
		}
		return new ResponseParamDTO<Map<String,Object>>();
	}
	
	/**
	 *This will generate the query with their respective data 
	 */
	public ResponseParamDTO<Map<String, Object>> generateQuery(String data, String sessionToken, Map<String, Object> userMap) throws Exception {

		/**
		 * validate API Directly from Gooru API permanently disabled since we
		 * have Redis server support but maintaining for backup.
		 * Map<String,Object> userMap = itemService.getUserObject(sessionToken,
		 * errorMap);
		 */
		if (userMap == null) {
			userMap = getUserObjectData(sessionToken);
		}

		RequestParamsDTO requestParamsDTO = baseAPIService.buildRequestParameters(data);

		Map<String, Boolean> checkPoint = baseAPIService.checkPoint(requestParamsDTO);

		/**
		 * Additional filters are added based on user authentication
		 */
		requestParamsDTO = baseAPIService.validateUserRole(requestParamsDTO, userMap);
		
		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		ResponseParamDTO<Map<String, Object>> responseParamDTO = esService.generateQuery(requestParamsDTO, indices, checkPoint);
		/**
		 * save data to redis
		 */
		responseParamDTO.setMessage(saveQuery(requestParamsDTO, responseParamDTO, data, userMap));
		return responseParamDTO;

	}

	public ResponseParamDTO<Map<String,String>> clearQuery(String id) {
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		Map<String, String> dataMap = new HashMap<String, String>();
		String message = APIConstants.EMPTY;
		
		if(baseAPIService.checkNull(id)){
		if(redisService.clearQuery(id)){
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

	@SuppressWarnings("unchecked")
	public ResponseParamDTO<Map<String,Object>> getQuery(String id, String sessionToken) {

		Map<String,Object> dataMap = getUserObjectData(sessionToken); 
		String prefix = APIConstants.EMPTY;
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		 if(dataMap.containsKey(APIConstants.GOORUUID) && dataMap.get(APIConstants.GOORUUID) != null){
			 prefix = dataMap.get(APIConstants.GOORUUID).toString()+APIConstants.SEPARATOR;
		 }
		String result = redisService.getQuery(prefix,id);
		if(result != null){
			
			responseParamDTO = baseAPIService.deserialize(result, responseParamDTO.getClass());
		}
		return responseParamDTO;
	} 

	
	
	public ResponseParamDTO<Map<String,Object>> getCacheData(String id,String sessionToken) {

		Map<String,Object> userMap = getUserObjectData(sessionToken);
		
		 String prefix = APIConstants.EMPTY;
		 if(userMap.containsKey(APIConstants.GOORUUID) && userMap.get(APIConstants.GOORUUID) != null){
			 prefix = userMap.get(APIConstants.GOORUUID).toString()+APIConstants.SEPARATOR;
		 }
		 ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		 List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			if (baseAPIService.checkNull(id)) {
				for (String requestId : id.split(APIConstants.COMMA)) {
					do {
						requestId = appendQuery(requestId, prefix, resultList);
					} while (redisService.hasKey(prefix+requestId));
				}
			} else {
				Set<String> keyIds = redisService.getKeys();
				Set<String> customizedKey = new HashSet<String>();
				for (String keyId : keyIds) {
					if (keyId.contains(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + APIConstants.CACHE_PREFIX_ID+prefix)) {
					customizedKey.add(keyId.replaceAll(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR+prefix, ""));
					}else{
					customizedKey.add(keyId.replaceAll(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR+prefix, ""));
					}
				}
				for (String requestId : customizedKey) {
					appendQuery(requestId, prefix, resultList);
				}
			}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.REDIS_MESSAGE.replace(ErrorConstants.REPLACER, ErrorConstants.GET));
		}
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}

	private String appendQuery(String requestId, String prefix, List<Map<String, Object>> resultList) {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put(requestId, redisService.getValue(prefix + requestId));
		requestId = redisService.getValue(prefix + requestId);
		resultList.add(dataMap);
		return requestId;
	}
	
	public ResponseParamDTO<Map<Integer,String>> manageReports(String action,String reportName,String data){
		
		 ResponseParamDTO<Map<Integer,String>> responseParamDTO = new ResponseParamDTO<Map<Integer,String>>();
		 Map<Integer,String> resultMap = new HashMap<Integer, String>();
		if(action.equalsIgnoreCase("add")){
			Column<String> val = baseCassandraService.readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS,reportName);
			
			if(val == null || (val !=null && StringUtils.isBlank(val.getStringValue()))){
					RequestParamsDTO requestParamsDTO = baseAPIService.buildRequestParameters(data);
				
				UUID reportId = UUID.randomUUID();
	
				baseCassandraService.saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS, reportName, reportId.toString());
				baseCassandraService.saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), reportId.toString(), APIConstants.QUERY, data);

				resultMap.put(200,ErrorConstants.SUCCESSFULLY_ADDED);
			}else{
				throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E105));
			}
		}else if(action.equalsIgnoreCase(APIConstants.UPDATE)){
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

	/**
	 * Depricated:used to fetch data from session token
	 * @param sessionToken
	 * @param errorMap
	 * @return
	 */
	private Map<String, Object> getUserObject(String sessionToken, Map<Integer, String> errorMap) {
		return baseConnectionService.getUserObject(sessionToken, errorMap);
	}

	private Map<String, Object> getUserObjectData(String sessionToken) {
		return baseConnectionService.getUserObjectData(sessionToken);
	}

	public ResponseParamDTO<Map<String,String>> insertKey(String data){
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		Map<String,String> resultData = new HashMap<String, String>();
		if(redisService.insertKey(data)){
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
	
	private Map<String,Object> saveQuery(RequestParamsDTO requestParamsDTO, ResponseParamDTO<Map<String,Object>> responseParamDTO, String data, Map<String, Object> userMap){
		try {
		if (requestParamsDTO.isSaveQuery() != null) {
			if (requestParamsDTO.isSaveQuery()) {
				String queryId = redisService.putCache(data,userMap, responseParamDTO);
				Map<String,Object> dataMap = new HashMap<String, Object>();
				dataMap.put(APIConstants.QUERY_ID, queryId);
				return dataMap;
			}
		}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.REDIS_MESSAGE.replace(ErrorConstants.REPLACER, ErrorConstants.INSERT));
		}
		return new HashMap<String, Object>();
	}
}
