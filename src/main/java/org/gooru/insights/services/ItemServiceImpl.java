package org.gooru.insights.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.util.IOUtils;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.gooru.insights.builders.utils.ExcludeNullTransformer;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.constants.APIConstants.Hasdatas;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ResponseParamDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

import flexjson.JSONSerializer;

@Service
public class ItemServiceImpl implements ItemService {
	
	@Autowired
	private BaseAPIService baseAPIService;

	@Autowired
	private BaseESService esService;

	@Autowired
	private RedisService redisService;
	
	@Autowired
	private ESDataProcessor businessLogicService;

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private BaseCassandraService baseCassandraService;
	
	@Autowired
	private CSVFileWriterService csvFileWriterService;
	
	@Autowired
	private MailService mailService;
	
	@Autowired
	private UserService userService;
	
	private static final Logger logger = LoggerFactory.getLogger(ItemServiceImpl.class);

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
	public ResponseParamDTO<Map<String,Object>> processApi(String traceId,String data, String sessionToken) throws Exception {

		Map<String,Object> userMap = getUserObjectData(traceId,sessionToken); 
		List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>();
		RequestParamsCoreDTO requestParamsCoreDTO = getBaseAPIService().buildRequestParamsCoreDTO(data);
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		if (getBaseAPIService().checkNull(requestParamsCoreDTO.getRequestParamsDTO())) {
			List<RequestParamsDTO> requestParamsDTOs = requestParamsCoreDTO.getRequestParamsDTO();

			String previousAPIKey = null;
			for (RequestParamsDTO api : requestParamsDTOs) {
				if (!getBaseAPIService().checkNull(api)) {
					continue;
				}
				responseParamDTO = generateQuery(traceId,data,null, userMap);
				if (getBaseAPIService().checkNull(previousAPIKey)) {
					resultData = getBaseAPIService().leftJoin(resultData, responseParamDTO.getContent(), previousAPIKey, api.getApiJoinKey());
				}
			}
			if (getBaseAPIService().checkNull(requestParamsCoreDTO.getCoreKey())) {
				resultData = getBusinessLogicService().formatAggregateKeyValueJson(resultData, requestParamsCoreDTO.getCoreKey());
			}
		}
		responseParamDTO.setContent(resultData);
		return responseParamDTO;
	}
	
	/**
	 * 
	 */
	public ResponseParamDTO<Map<String,Object>> getPartyReport(String traceId,HttpServletRequest request,String reportType,String sessionToken) throws Exception {
		RequestParamsDTO systemRequestParamsDTO = null;
		boolean isMerged = false;
		JSONSerializer serializer = new JSONSerializer();

		Map<String, Object> userMap = getUserObjectData(traceId,sessionToken);
		
		Map<String,Object> filtersMap = getBaseAPIService().getRequestFieldNameValueInMap(request, APIConstants.F);
		Map<String,Object> paginationMap = getBaseAPIService().getRequestFieldNameValueInMap(request, APIConstants.P);
		
		if(filtersMap.isEmpty()){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100, APIConstants.FILTERS));
		}
		
		Column<String> val = getBaseCassandraService().readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(),APIConstants.DI_REPORTS,reportType);
		
		if(val == null){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E106));
		}
		
		ColumnList<String> columns = getBaseCassandraService().read(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue());
		
		systemRequestParamsDTO = getBaseAPIService().buildRequestParameters(columns.getStringValue(APIConstants.QUERY, null));
		for(RequestParamsFilterDetailDTO systemFieldData : systemRequestParamsDTO.getFilter()) {
			for(RequestParamsFilterFieldsDTO systemfieldsDetails : systemFieldData.getFields()) {
				if(filtersMap.containsKey(systemfieldsDetails.getFieldName())){
					isMerged = true;
					String[] values = filtersMap.get(systemfieldsDetails.getFieldName()).toString().split(APIConstants.COMMA);
					systemfieldsDetails.setValue(filtersMap.get(systemfieldsDetails.getFieldName()).toString());
					if(values.length > 1){
						systemfieldsDetails.setOperator(APIConstants.IN);
					}
				}
			}
		}
		if(!isMerged){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E107,APIConstants.FILTERS));
		}

		if(!paginationMap.isEmpty()){
			if(paginationMap.containsKey(APIConstants.LIMIT)){
				systemRequestParamsDTO.getPagination().setLimit(Integer.valueOf(APIConstants.EMPTY+paginationMap.get(APIConstants.LIMIT)));
			}
			if(paginationMap.containsKey(APIConstants.OFFSET)){
				systemRequestParamsDTO.getPagination().setOffset(Integer.valueOf(APIConstants.EMPTY+paginationMap.get(APIConstants.LIMIT)));
			}
			if(paginationMap.containsKey(APIConstants.SORT_ORDER)){
				for(RequestParamsSortDTO requestParamsSortDTO :   systemRequestParamsDTO.getPagination().getOrder()){
					requestParamsSortDTO.setSortOrder(paginationMap.get(APIConstants.SORT_ORDER).toString());
				}
			}
		}
		InsightsLogger.info(traceId, APIConstants.OLD_QUERY+columns.getStringValue(APIConstants.QUERY, null));

		serializer.transform(new ExcludeNullTransformer(), void.class).exclude(APIConstants.EXCLUDE_CLASSES);
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		InsightsLogger.info(traceId, BaseAPIServiceImpl.buildString(new Object[]{APIConstants.NEW_QUERY, datas}));
		
		if(columns.getStringValue(APIConstants.QUERY, null) != null){			
			return generateQuery(traceId,datas, null, userMap);
		}
		return new ResponseParamDTO<Map<String,Object>>();
	}
	
	/**
	 *This will generate the query with their respective data 
	 */
	public ResponseParamDTO<Map<String, Object>> generateQuery(String traceId,String data, String sessionToken, Map<String, Object> userMap) throws Exception {

		/**
		 * validate API Directly from Gooru API permanently disabled since we
		 * have Redis server support but maintaining for backup.
		 * Map<String,Object> userMap = itemService.getUserObject(sessionToken,
		 * errorMap);
		 */
		if (userMap == null) {
//			userMap = getUserObjectData(traceId,sessionToken);
		}

		RequestParamsDTO requestParamsDTO = getBaseAPIService().buildRequestParameters(data);

		Map<String, Boolean> checkPoint = getBaseAPIService().checkPoint(requestParamsDTO);

		/**
		 * Additional filters are added based on user authentication
		 */
		requestParamsDTO = getUserService().validateUserRole(traceId,requestParamsDTO, userMap);
		
		String[] indices = getBaseAPIService().getIndices(requestParamsDTO.getDataSource().toLowerCase());
		ResponseParamDTO<Map<String, Object>> responseParamDTO = getEsService().generateQuery(traceId,requestParamsDTO, indices, checkPoint);
		/**
		 * save data to redis
		 */
		responseParamDTO.setMessage(saveQuery(traceId,requestParamsDTO, responseParamDTO, data, userMap));
		return responseParamDTO;

	}

	public ResponseParamDTO<Map<String,String>> clearQuery(String traceId,String id) {
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		Map<String, String> dataMap = new HashMap<String, String>();
		String message = APIConstants.EMPTY;
		
		if(getBaseAPIService().checkNull(id)){
		if(getRedisService().clearQuery(id)){
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
	public ResponseParamDTO<Map<String,Object>> getQuery(String traceId,String id, String sessionToken) {

		Map<String,Object> dataMap = getUserObjectData(traceId,sessionToken); 
		String prefix = APIConstants.EMPTY;
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		 if(dataMap.containsKey(APIConstants.GOORUUID) && dataMap.get(APIConstants.GOORUUID) != null){
			 prefix = dataMap.get(APIConstants.GOORUUID).toString()+APIConstants.SEPARATOR;
		 }
		String result = getRedisService().getQuery(prefix,id);
		if(result != null){
			
			responseParamDTO = getBaseAPIService().deserialize(result, responseParamDTO.getClass());
		}
		return responseParamDTO;
	} 

	
	
	public ResponseParamDTO<Map<String,Object>> getCacheData(String traceId,String id,String sessionToken) {

		Map<String,Object> userMap = getUserObjectData(traceId,sessionToken);
		
		 String prefix = APIConstants.EMPTY;
		 if(userMap.containsKey(APIConstants.GOORUUID) && userMap.get(APIConstants.GOORUUID) != null){
			 prefix = userMap.get(APIConstants.GOORUUID).toString()+APIConstants.SEPARATOR;
		 }
		 ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		 List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			if (getBaseAPIService().checkNull(id)) {
				for (String requestId : id.split(APIConstants.COMMA)) {
					do {
						requestId = appendQuery(requestId, prefix, resultList);
					} while (getRedisService().hasKey(BaseAPIServiceImpl.buildString(new Object[]{prefix, requestId})));
				}
			} else {
				Set<String> keyIds = getRedisService().getKeys();
				Set<String> customizedKey = new HashSet<String>();
				for (String keyId : keyIds) {
					if (keyId.contains(BaseAPIServiceImpl.buildString(new Object[]{APIConstants.CACHE_PREFIX, APIConstants.SEPARATOR, APIConstants.CACHE_PREFIX_ID, prefix}))) {
					customizedKey.add(keyId.replaceAll(BaseAPIServiceImpl.buildString(new Object[]{APIConstants.CACHE_PREFIX, APIConstants.SEPARATOR, APIConstants.CACHE_PREFIX_ID, APIConstants.SEPARATOR, prefix}), APIConstants.EMPTY));
					}else{
					customizedKey.add(keyId.replaceAll(BaseAPIServiceImpl.buildString(new Object[]{APIConstants.CACHE_PREFIX, APIConstants.SEPARATOR, prefix}), APIConstants.EMPTY));
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
		dataMap.put(requestId, getRedisService().getValue(BaseAPIServiceImpl.buildString(new Object[]{prefix, requestId})));
		requestId = getRedisService().getValue(BaseAPIServiceImpl.buildString(new Object[]{prefix, requestId}));
		resultList.add(dataMap);
		return requestId;
	}
	
	public ResponseParamDTO<Map<Integer,String>> manageReports(String traceId,String action,String reportName,String data){
		
		 ResponseParamDTO<Map<Integer,String>> responseParamDTO = new ResponseParamDTO<Map<Integer,String>>();
		 Map<Integer,String> resultMap = new HashMap<Integer, String>();
		if(action.equalsIgnoreCase(APIConstants.ADD)){
			Column<String> val = getBaseCassandraService().readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS,reportName);
			
			if(val == null || (val !=null && StringUtils.isBlank(val.getStringValue()))){
					RequestParamsDTO requestParamsDTO = getBaseAPIService().buildRequestParameters(data);
				
				UUID reportId = UUID.randomUUID();
	
				getBaseCassandraService().saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS, reportName, reportId.toString());
				getBaseCassandraService().saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), reportId.toString(), APIConstants.QUERY, data);

				resultMap.put(200,ErrorConstants.SUCCESSFULLY_ADDED);
			}else{
				throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E105));
			}
		}else if(action.equalsIgnoreCase(APIConstants.UPDATE)){
			Column<String> val = getBaseCassandraService().readColumnValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), APIConstants.DI_REPORTS,reportName);
			
			if(val !=null && !StringUtils.isBlank(val.getStringValue())){
				try {
					RequestParamsDTO requestParamsDTO = getBaseAPIService().buildRequestParameters(data);
				} catch (Exception e) {
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E102,new String[]{APIConstants.JSON_FORMAT}));
					
//					errorMap.put(400,E1014);
//					return errorMap;
				}	
				getBaseCassandraService().saveStringValue(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue(), APIConstants.QUERY, data);
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
		return getBaseConnectionService().getUserObject(sessionToken, errorMap);
	}

	private Map<String, Object> getUserObjectData(String traceId,String sessionToken) {
		return getBaseConnectionService().getUserObjectData(traceId,sessionToken);
	}

	public ResponseParamDTO<Map<String,String>> insertKey(String traceId,String data){
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		Map<String,String> resultData = new HashMap<String, String>();
		if(getRedisService().insertKey(data)){
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
		getBaseConnectionService().clearDataCache();
		dataMap.put(MessageHandler.getMessage(APIConstants.STATUS_NAME), MessageHandler.getMessage(APIConstants.CACHE_CLEAR).replace(ErrorConstants.REPLACER, MessageHandler.getMessage(APIConstants.DATA)));	
		responseParamDTO.setMessage(dataMap);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String,Object>> clearConnectionCache() {
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		Map<String,Object> dataMap = new HashMap<String,Object>();
		getBaseConnectionService().clearConnectionCache();
		dataMap.put(MessageHandler.getMessage(APIConstants.STATUS_NAME), MessageHandler.getMessage(APIConstants.CACHE_CLEAR).replace(ErrorConstants.REPLACER, MessageHandler.getMessage(APIConstants.CONNECTION)));	
		responseParamDTO.setMessage(dataMap);
		return responseParamDTO;
	}
	
	private Map<String,Object> saveQuery(String traceId,RequestParamsDTO requestParamsDTO, ResponseParamDTO<Map<String,Object>> responseParamDTO, String data, Map<String, Object> userMap){
		try {
		if (requestParamsDTO.isSaveQuery() != null) {
			if (requestParamsDTO.isSaveQuery()) {
				String queryId = getRedisService().putCache(traceId,data,userMap, responseParamDTO);
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
	
	public void generateReport(String traceId, String data, String sessionToken, Map<String, Object> userMap, String absoluteFilePath) {

		String delimiter = getBaseConnectionService().getExportReportCache().get(APIConstants.DELIMITER);
		int defaultLimit = Integer.valueOf(getBaseConnectionService().getExportReportCache().get(APIConstants.DEFAULT_LIMIT));
		int offSet = 0;
		int totalRows = 0;
		boolean isNewFile = true;
		try {
			
			RequestParamsDTO requestParamsDTO = getBaseAPIService().buildRequestParameters(data);
			RequestParamsPaginationDTO paginationDTO = requestParamsDTO.getPagination();
			ResponseParamDTO<Map<String, Object>> responseDTO = null;
			
			offSet = paginationDTO.getOffset();
			paginationDTO.setLimit(defaultLimit);
			requestParamsDTO.setPagination(paginationDTO);
			
			if (userMap == null) {
				userMap = getUserObjectData(traceId,sessionToken);
			}

			Map<String, Boolean> checkPoint = getBaseAPIService().checkPoint(requestParamsDTO);
			requestParamsDTO = getUserService().validateUserRole(traceId,requestParamsDTO, userMap);
			String[] indices = getBaseAPIService().getIndices(requestParamsDTO.getDataSource().toLowerCase());
			
			do {
				responseDTO = getEsService().generateQuery(traceId,requestParamsDTO, indices, checkPoint);
				getCSVFileWriterService().generateCSVReport(new HashSet<String>(Arrays.asList(requestParamsDTO.getFields().split(APIConstants.COMMA))), responseDTO.getContent(), absoluteFilePath, delimiter, isNewFile);
				checkPoint.put(Hasdatas.HAS_MULTIGET.check(), false);
				/*Incrementing offset values */
				offSet += defaultLimit;
				paginationDTO.setOffset(offSet);
				requestParamsDTO.setPagination(paginationDTO);
				
				checkPoint = getBaseAPIService().checkPoint(requestParamsDTO);
				totalRows = Integer.valueOf(responseDTO.getPaginate().get(APIConstants.TOTAL_ROWS).toString());
				isNewFile = false;
			} while(offSet < totalRows);
			
		} catch (Exception e) {
			logger.error("Error while writting file. {}", e);
		}
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> exportReport(HttpServletResponse response, String traceId, String data, String sessionToken, Map<String, Object> userMap) {

		int maxLimit = Integer.valueOf(getBaseConnectionService().getExportReportCache().get(APIConstants.MAX_LIMIT));
		int totalRows = 0;
		String absoluteFilePath = null;
		String fileName = UUID.randomUUID().toString();
		ResponseParamDTO<Map<String, Object>> responseDTO = null;
		
		try {
			
			RequestParamsDTO requestParamsDTO = getBaseAPIService().buildRequestParameters(data);
			
			if (userMap == null) {
				userMap = getUserObjectData(traceId,sessionToken);
			}

			Map<String, Boolean> checkPoint = getBaseAPIService().checkPoint(requestParamsDTO);
			requestParamsDTO = getUserService().validateUserRole(traceId,requestParamsDTO, userMap);
			String[] indices = getBaseAPIService().getIndices(requestParamsDTO.getDataSource().toLowerCase());

			totalRows = Integer.valueOf(getEsService().generateQuery(traceId,requestParamsDTO, indices, checkPoint).getPaginate().get(APIConstants.TOTAL_ROWS).toString());
			absoluteFilePath = getBaseConnectionService().getRealRepoPath().concat(fileName).concat(APIConstants.DOT).concat(APIConstants.CSV_EXTENSION);
			
			Map<String, Object> status = new HashMap<String, Object>();
			final String resultLink = getBaseConnectionService().getAppRepoPath().concat(fileName).concat(APIConstants.DOT).concat(APIConstants.CSV_EXTENSION);
			responseDTO = new ResponseParamDTO<Map<String,Object>>();
			
			if(totalRows > maxLimit) {
				final String traceID = traceId;
				final String query = data;
				final String session = sessionToken;
				final Map<String, Object> user = userMap;
				final String filePath = absoluteFilePath;

				final Thread reportThread = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							generateReport(traceID, query, session, user, filePath);
							getMailService().sendMail(user.get(APIConstants.EXTERNAL_ID).toString(), "Query Report", "Please download the attachement ", resultLink);
						}
						catch(Exception e) {
							logger.error("Exception in export report[Thread Loop]: {}", e);
						}
					}
				});
				reportThread.setDaemon(true);
				reportThread.start();
				status.put(APIConstants.STATUS_NAME.toLowerCase(), MessageHandler.getMessage(APIConstants.FILE_MAILED));
				
			} else {
				generateReport(traceId, data, sessionToken, userMap, absoluteFilePath);
				status.put(APIConstants.FILE_PATH, absoluteFilePath);
			}
			responseDTO.setMessage(status);

		} catch (BadRequestException badRequestException) {
			throw badRequestException;
		} catch (Exception exception) {
			logger.error("Error while writting file. {}", exception);
		}

		return responseDTO;
	}

	public BaseAPIService getBaseAPIService() {
		return baseAPIService;
	}

	public BaseESService getEsService() {
		return esService;
	}

	public RedisService getRedisService() {
		return redisService;
	}

	public ESDataProcessor getBusinessLogicService() {
		return businessLogicService;
	}

	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}

	public BaseCassandraService getBaseCassandraService() {
		return baseCassandraService;
	}

	public UserService getUserService() {
		return userService;
	}
	
	public MailService getMailService() {
		return mailService;
	}
	
	public CSVFileWriterService getCSVFileWriterService() {
		return csvFileWriterService;
	}
}
