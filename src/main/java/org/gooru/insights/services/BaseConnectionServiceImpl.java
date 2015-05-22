package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Component
public class BaseConnectionServiceImpl implements BaseConnectionService {

	private static Client devClient;
	
	private static Client prodClient;
	
	private static Keyspace insightsKeyspace;
	
	private static String arrayHandler;
	
	private static Map<String,String> fieldsDataTypeCache;
	
	private static Map<String,Map<String,String>>  fieldsCustomDataTypeCache;
	
	private static Map<String,Map<String,String>> fieldsConfigCache;
	
	private static Map<String,Map<String,String>> fieldsCache;
	
	private static Map<String,Map<String,Map<String, String>>> dependentFieldsCache;
	
	private static Map<String,String> fieldArrayHandler;
	
	private static Map<String,String> indexMap;
	
	private static Map<String,String> defaultFields;
	
	private static Map<String, Map<String, String>> apiFields;
	
	private static Set<String> logicalOperations;
	
	private static Set<String> esOperations;
	
	private static Set<String> formulaOperations;
	
	private static Set<String> dataTypes;
	
	private static Map<String, String> exportFieldCache;
	
	private static Map<String, Object> exportReportCache;
	
	private static Map<String, ColumnList<String>> columnListCache;
	
	@Autowired
	private BaseAPIService baseAPIService;
	
	@Autowired
	private  BaseCassandraService baseCassandraService;
	
	@Resource(name="filePath")
	private Properties fileProperties;
	
	@Autowired
	private RedisService redisService;
	
	@PostConstruct
	private void initConnect() {

		if (insightsKeyspace == null) {
			initCassandraConnection();
		}
		if (devClient == null) {
			initDevESConnection();
		}
		if (prodClient == null) {
			initProdESConnection();
		}
		if (indexMap == null) {
			columnListCache = new HashMap<String, ColumnList<String>>();
			putLogicalOperations();
			putFormulas();
			putEsOperations();
			putDataTypes();
			indexList();
			dependentFields();
			fieldDataType();
			fieldsConfig();
			fieldArrayHandler();
			apiFields();
			exportConfig();
		}
	}
	
	private void initCassandraConnection() {

		String seeds = getFileProperties().getProperty(CassandraConstants.CassandraConfigs.SEEDS.cassandraConfig());
		Integer port = Integer.parseInt(getFileProperties().getProperty(CassandraConstants.CassandraConfigs.PORT.cassandraConfig()));
		String clusterName = getFileProperties().getProperty(CassandraConstants.CassandraConfigs.CLUSTER.cassandraConfig());
		String insights = getFileProperties().getProperty(CassandraConstants.CassandraConfigs.INSIGHTS_KEYSPACE.cassandraConfig());
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(clusterName).forKeyspace(insights)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE).setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
				.withConnectionPoolConfiguration(connectionConfig(seeds, port)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		insightsKeyspace = (Keyspace) context.getClient();

	}
	
	private ConnectionPoolConfigurationImpl connectionConfig(String seeds, Integer port) {
		StringBuffer seedConfigured = new StringBuffer();
		for (String seed : seeds.split(APIConstants.COMMA)) {
			if (seedConfigured.length() > 0) {
				seedConfigured.append(APIConstants.COMMA);
			}
			seedConfigured.append(seed + APIConstants.COLON + port);
		}
		ConnectionPoolConfigurationImpl connectionPoolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(port.intValue()).setMaxConnsPerHost(2)
				.setSeeds(seedConfigured.toString());
		if (!seeds.startsWith("127.0")) {
			connectionPoolConfig.setLocalDatacenter(getFileProperties().getProperty(CassandraConstants.CassandraConfigs.DATA_CENTRE.cassandraConfig()));
		}
		// connectionPoolConfig.setLatencyScoreStrategy(new
		// SmaLatencyScoreStrategyImpl()); // Enabled SMA. Omit this to use
		// round robin with a token range
		return connectionPoolConfig;
	}
	
	private void initDevESConnection(){
		OperationResult<ColumnList<String>> rowResult = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(),ESConstants.EsConfigs.DEV_ROWKEY.esConfig(), new ArrayList<String>());
		ColumnList<String> columnList = rowResult.getResult();
		String clusterName = columnList.getStringValue(ESConstants.EsConfigs.CLUSTER.esConfig(),APIConstants.EMPTY) ;
		String hostName = columnList.getColumnByName(ESConstants.EsConfigs.HOSTS.esConfig()).getStringValue();
		String portNo = columnList.getColumnByName(ESConstants.EsConfigs.PORTNO.esConfig()).getStringValue();
		String nodeType = columnList.getColumnByName(ESConstants.EsConfigs.NODE.esConfig()).getStringValue();
		if(nodeType != null && !nodeType.isEmpty()){
		if(ESConstants.EsConfigs.NODE_CLIENT.esConfig().equalsIgnoreCase(nodeType)){
			devClient  = initNodeClient(clusterName);
		}
		}
		if(!baseAPIService.checkNull(devClient)){
			devClient = initTransportClient(hostName,portNo,clusterName);
		}
	}

	private void initProdESConnection(){
		OperationResult<ColumnList<String>> rowResult = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(),ESConstants.EsConfigs.ROWKEY.esConfig(), new ArrayList<String>());
		ColumnList<String> columnList = rowResult.getResult();
		String clusterName = columnList.getStringValue(ESConstants.EsConfigs.CLUSTER.esConfig(),APIConstants.EMPTY) ;
		String hostName = columnList.getColumnByName(ESConstants.EsConfigs.HOSTS.esConfig()).getStringValue();
		String portNo = columnList.getColumnByName(ESConstants.EsConfigs.PORTNO.esConfig()).getStringValue();
		String nodeType = columnList.getColumnByName(ESConstants.EsConfigs.NODE.esConfig()).getStringValue();
		if(nodeType != null && !nodeType.isEmpty()){
		if(ESConstants.EsConfigs.NODE_CLIENT.esConfig().equalsIgnoreCase(nodeType)){
			prodClient  = initNodeClient(clusterName);
		}
		}
		if(!baseAPIService.checkNull(prodClient)){
			prodClient = initTransportClient(hostName,portNo,clusterName);
		}
	}
	
	
	private Client initNodeClient(String clusterName){
		 Settings settings = ImmutableSettings.settingsBuilder().put(ESConstants.EsConfigs.ES_CLUSTER.esConfig(), clusterName != null ? clusterName : APIConstants.EMPTY).put("client.transport.sniff", true).build();
		 return new NodeBuilder().settings(settings).node().client();   	
	}
	
	private Client initTransportClient(String hostName,String portNo,String clusterName){
		 Settings settings = ImmutableSettings.settingsBuilder().put(ESConstants.EsConfigs.ES_CLUSTER.esConfig(), clusterName != null ? clusterName : APIConstants.EMPTY).put("client.transport.sniff", true).build();
         TransportClient transportClient = new TransportClient(settings);
         transportClient.addTransportAddress(new InetSocketTransportAddress(hostName, Integer.valueOf(portNo)));
         return transportClient;
	}
	
	private void fieldDataType(){
		
		fieldsDataTypeCache = new HashMap<String, String>();
		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.EVENT_FIELDS.columnFamily(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		for(Row<String, String> row : rows){
			fieldsDataTypeCache.put(row.getKey(),row.getColumns().getStringValue("description",row.getKey()));
		}
	}
	
	private Set<String> indexList(){

		indexMap = new HashMap<String,String>();
		Set<String> fetchFields = new HashSet<String>(); 
		OperationResult<ColumnList<String>> rowResult =baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(),ESConstants.EsConfigs.ES_INDICES.esConfig(), new ArrayList<String>());
		for(Column<String> column :  rowResult.getResult()){
			indexMap.put(column.getName(), column.getStringValue());
			fetchFields.add(column.getStringValue());
		}
		return fetchFields;
	}
	
	private void fieldsConfig(){

		fieldsConfigCache = new HashMap<String, Map<String,String>>();
		fieldsCache = new HashMap<String,Map<String,String>>();
		fieldsCustomDataTypeCache = new HashMap<String,Map<String,String>>();
		defaultFields = new HashMap<String,String>();
		
		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(),indexList(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		
		for(Row<String,String> row : rows){
			Map<String,String> configMap = new HashMap<String, String>();
			Map<String,String> fieldsMap = new HashMap<String, String>();
			Map<String,String> fieldsDataTypeMap = new HashMap<String, String>();
			if(row.getColumns().getColumnByName("common_fields") != null ){
			configMap.put(row.getColumns().getColumnByName("common_fields").getStringValue(), row.getColumns().getColumnByName("fetch_fields") != null ? row.getColumns().getColumnByName("fetch_fields").getStringValue() : null);
			defaultFields.put(row.getKey(),row.getColumns().getColumnByName("fetch_fields") != null ? row.getColumns().getColumnByName("fetch_fields").getStringValue() : null);
			}
			putColumnListDataToMap(row.getColumns(), fieldsMap);
			OperationResult<ColumnList<String>> result = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(),row.getKey()+"~datatype",new ArrayList<String>());
			putColumnListDataToMap(result.getResult(), fieldsDataTypeMap);
			fieldsCustomDataTypeCache.put(row.getKey(), fieldsDataTypeMap);
			fieldsCache.put(row.getKey(),fieldsMap);
			fieldsConfigCache.put(row.getKey(), configMap);
		}
	}

	private void dependentFields(){
		
		dependentFieldsCache = new HashMap<String,Map<String,Map<String, String>>>();
		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(),indexList(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		for(Row<String,String> row : rows){
			Map<String,Map<String,String>> dataSet = new HashMap<String, Map<String,String>>();
			if(row.getColumns().getColumnByName("dependent_fields") != null){
			 String dependentKeys = row.getColumns().getColumnByName("dependent_fields").getStringValue();
			 Set<String> fetchFields = new HashSet<String>(); 
				for(String dependentKey : dependentKeys.split(APIConstants.COMMA)){
					fetchFields.add(row.getKey()+APIConstants.SEPARATOR+dependentKey);
				}
				operationalResult =  baseCassandraService.readAll(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(), fetchFields, new ArrayList<String>());
				Rows<String,String> dependentrows = operationalResult.getResult();
				for(Row<String,String> dependentRow : dependentrows){
					Map<String,String> dependentMap = new HashMap<String, String>();
					putColumnListDataToMap(dependentRow.getColumns(), dependentMap);
					String[] key = dependentRow.getKey().split(APIConstants.SEPARATOR);
					dataSet.put(key[1], dependentMap);
				}
				dependentFieldsCache.put(row.getKey(), dataSet);
			}
		}
	}
	
	public void apiFields(){
		Map<String, Map<String, String>> indexDbFields = getFields();
		apiFields = new HashMap<String, Map<String, String>>();
		for(Map.Entry<String, Map<String, String>> indexList : indexDbFields.entrySet()){
		Map<String, String> fields = new HashMap<String, String>();
		for(Map.Entry<String, String> dbFields : indexList.getValue().entrySet()){
			fields.put(dbFields.getValue(), dbFields.getKey());
		}
		apiFields.put(indexList.getKey(), fields);
		}
	}
	
	private void fieldArrayHandler(){
		fieldArrayHandler = new HashMap<String, String>();
		arrayHandler = new String();
		ColumnList<String> operationalResult = baseCassandraService.read(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(), CassandraConstants.CassandraRowKeys.FILED_ARRAY_HANDLER.CassandraRowKey());
		Collection<String> columnList = operationalResult.getColumnNames();
		arrayHandler = baseAPIService.convertCollectiontoString(columnList);
		putColumnListDataToMap(operationalResult, fieldArrayHandler);
	}
	
	private void exportConfig() {
		ColumnList<String> exportFields = baseCassandraService.read(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(), CassandraConstants.CassandraRowKeys.EXPORT_FIELDS.CassandraRowKey());
		columnListCache.put(CassandraConstants.CassandraRowKeys.EXPORT_FIELDS.CassandraRowKey(), exportFields);

		ColumnList<String> reportExportConfig = baseCassandraService.read(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(), CassandraConstants.CassandraRowKeys.EXPORT_REPORT_CONFIG.CassandraRowKey());
		columnListCache.put(CassandraConstants.CassandraRowKeys.EXPORT_REPORT_CONFIG.CassandraRowKey(), reportExportConfig);
		
		ColumnList<String> liveDashBoardFieldsConfig = baseCassandraService.read(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.CONFIG_SETTINGS.columnFamily(), CassandraConstants.CassandraRowKeys.LIVE_DASHBOARD.CassandraRowKey());
		columnListCache.put(CassandraConstants.CassandraRowKeys.LIVE_DASHBOARD.CassandraRowKey(), liveDashBoardFieldsConfig);
	}
	
	private void putColumnListDataToMap(ColumnList<String> columnList, Map<String, String> map) {
		if(map != null) {
			for(Column<String> column : columnList) {
				map.put(column.getName(), column.getStringValue());
			}
		} else {
			throw new NullPointerException();
		}
	}
	
	public void clearDataCache(){
		indexMap = new HashMap<String,String>();
		dependentFieldsCache = new HashMap<String,Map<String,Map<String, String>>>();
		fieldsDataTypeCache = new HashMap<String, String>();
		fieldsConfigCache = new HashMap<String, Map<String,String>>();
		fieldsCache = new HashMap<String,Map<String,String>>();
		fieldsCustomDataTypeCache = new HashMap<String,Map<String,String>>();
		defaultFields = new HashMap<String,String>();
		fieldArrayHandler = new HashMap<String, String>();
		arrayHandler = new String();
		indexList();
		dependentFields();
		fieldDataType();
		fieldsConfig();
		fieldArrayHandler();
		apiFields();
		exportConfig();
	}
	
	public void clearConnectionCache(){
		insightsKeyspace = null;
		devClient = null;
		prodClient = null;
		initCassandraConnection();
		initDevESConnection();
		initProdESConnection();
	}
	
	/**
	 * Depricated
	 */
	public Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap){
		ColumnList<String> endPoint = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),"gooru.api.rest.endpoint", new ArrayList<String>()).getResult();
		Map<String,Object> userMap = new LinkedHashMap<String, Object>();		
		String address = endPoint.getColumnByName("constant_value").getStringValue()+"/v2/user/token/"+ sessionToken + "?sessionToken=" + sessionToken;
		ClientResource client = new ClientResource(address);
		if (client.getStatus().isSuccess()) {
			try{
				Representation representation = client.get();
				JsonRepresentation jsonRepresentation = new JsonRepresentation(
						representation);
				JSONObject jsonObj = jsonRepresentation.getJsonObject();
				userMap.put("firstName",jsonObj.getString("firstName"));
				userMap.put("lastName",jsonObj.getString("lastName"));
				userMap.put("externalId",jsonObj.getString("externalId"));
				userMap.put("gooruUId",jsonObj.getString("gooruUId"));
				userMap.put("userRoleSetString",jsonObj.getString("userRoleSetString"));
			}catch(Exception e){
				errorMap.put(500, e.toString());
			}
		}else{
			errorMap.put(500, "We're unable to get User information");
		}
		
		return userMap;
		
	}
	
	public Map<String, Object> getUserObjectData(String traceId,String sessionToken) {

		String result = redisService.getDirectValue(APIConstants.GOORU_PREFIX + sessionToken);
		Map<String, Object> userMap = new LinkedHashMap<String, Object>();
		try {
			JSONObject coreJsonObject = new JSONObject(result);
			Set<String> permissionSet = new HashSet<String>();
			Map<String, Set<String>> partyPermissions = new HashMap<String, Set<String>>();

			JSONObject jsonObject = new JSONObject(coreJsonObject.getString(APIConstants.USER_TOKEN));
			jsonObject = new JSONObject(jsonObject.getString(APIConstants.USER));
			userMap.put(APIConstants.FIRST_NAME, jsonObject.getString(APIConstants.FIRST_NAME));
			userMap.put(APIConstants.LAST_NAME, jsonObject.getString(APIConstants.LAST_NAME));
			userMap.put(APIConstants.EXTERNAL_ID, jsonObject.getJSONArray(APIConstants.IDENTITIES).getJSONObject(0).getString(APIConstants.EXTERNAL_ID));
			userMap.put(APIConstants.GOORUUID, jsonObject.getString(APIConstants.PARTY_UID));
			userMap.put(APIConstants.USER_ROLE_SETSTRING, jsonObject.getString(APIConstants.USER_ROLE_SETSTRING));

			JSONObject userCredential = new JSONObject(coreJsonObject.getString(APIConstants.USER_CREDENTIAL));
			JSONObject partyPermissionList = new JSONObject(userCredential.getString(APIConstants.PARTY_PERMISSIONS));
			Iterator<String> keys = partyPermissionList.keys();

			while (keys.hasNext()) {
				String key = keys.next();
				JSONArray jsonArray = partyPermissionList.getJSONArray(key);
				for (int i = 0; i < jsonArray.length(); i++) {
					permissionSet.add(jsonArray.getString(i));
				}
				partyPermissions.put(key, permissionSet);
			}
			InsightsLogger.info(traceId, APIConstants.PARTY_PERMISSION_MESSAGE + permissionSet);
			userMap.put(APIConstants.PERMISSIONS, partyPermissions);
		} catch (Exception e) {
			throw new ReportGenerationException(MessageHandler.getMessage(ErrorConstants.E101));
		}
		return userMap;
	}

	private void putLogicalOperations(){
		logicalOperations = new HashSet<String>();
		logicalOperations.add("AND");
		logicalOperations.add("OR");
		logicalOperations.add("NOT");
	}
	
	private void putFormulas(){
		formulaOperations = new HashSet<String>();
		formulaOperations.add("SUM");
		formulaOperations.add("MIN");
		formulaOperations.add("AVG");
		formulaOperations.add("MAX");
		formulaOperations.add("COUNT");
		formulaOperations.add("DISTINCT");
		formulaOperations.add("PERCENTILES");
	}
	
	private void putDataTypes(){
		dataTypes = new HashSet<String>();
		dataTypes.add("INTEGER");
		dataTypes.add("LONG");
		dataTypes.add("STRING");
		dataTypes.add("DATE");
		dataTypes.add("DOUBLE");
	}
	
	private void putEsOperations(){
		esOperations = new HashSet<String>();
		esOperations.add("GT");
		esOperations.add("RG");
		esOperations.add("NRG");
		esOperations.add("EQ");
		esOperations.add("LK");
		esOperations.add("EX");
		esOperations.add("IN");
		esOperations.add("LE");
		esOperations.add("GE");
		esOperations.add("LT");
		esOperations.add("gt");
	}
	
	public Set<String> getLogicalOperations(){
		return logicalOperations; 
	}
	
	public Set<String> getEsOperations(){
		return esOperations; 
	}
	
	public Map<String,String> getDefaultFields(){
		return defaultFields;
	}
	
	public String getArrayHandler() {
		return arrayHandler;
	}

	public  Map<String, String> getFieldArrayHandler() {
		return fieldArrayHandler;
	}

	public Properties getFileProperties() {
		return fileProperties;
	}	
	
	public Client getDevClient() {
		return devClient;
	}
	
	public Client getProdClient() {
		return prodClient;
	}
	
	public Keyspace connectInsights(){
		return insightsKeyspace;
	}
	
	public Map<String,Map<String,Map<String, String>>> getDependentFieldsCache() {
		return dependentFieldsCache;
	}

	public Map<String,Map<String,String>> getFields() {
		return fieldsCache;
	}
	
	public Set<String> getFormulaOperations() {
		return formulaOperations;
	}

	public Set<String> getDataTypes() {
		return dataTypes;
	}
	
	public Map<String, String> getFieldsDataType() {
		return fieldsDataTypeCache;
	}

	public Map<String, Map<String, String>> getFieldsJoinCache() {
		return fieldsConfigCache;
	}
	
	public Map<String, Map<String, String>>  getFieldsCustomDataType() {
		return fieldsCustomDataTypeCache;
	}
	
	public Map<String, String> getIndexMap() {
		return indexMap;
	}
	
	public Map<String, Map<String, String>> getApiFields(){
		return apiFields;
	}
	
	public String getRealRepoPath() {
		return fileProperties.getProperty(APIConstants.REPO_REAL_PATH);
	}
	
	public String getAppRepoPath() {
		return fileProperties.getProperty(APIConstants.REPO_APP_PATH);
	}
	
	public String getDefaultReplyToEmail() {
		return fileProperties.getProperty(APIConstants.DEFAULT_REPLYTO_MAIL);
	}
	
	public String getDefaultToEmail() {
		return fileProperties.getProperty(APIConstants.DEFAULT_TO_MAIL);
	}
	
	@Override
	public ColumnList<String> getColumnListFromCache(String rowKey) {
		return columnListCache.get(rowKey);
	}
}


