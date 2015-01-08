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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.aspectj.lang.ProceedingJoinPoint;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.CassandraConstants.columnFamilies;
import org.gooru.insights.constants.CassandraConstants.keyspaces;
import org.gooru.insights.constants.ESConstants.esConfigs;
import org.gooru.insights.models.User;
import org.gooru.insights.security.AuthorizeOperations;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Component
public class BaseConnectionServiceImpl implements BaseConnectionService,CassandraConstants,APIConstants {

	Logger loggerFactory = LoggerFactory.getLogger(BaseConnectionServiceImpl.class);
	private static Client devClient;
	
	private static Client prodClient;
	
	private static Keyspace insightsKeyspace;
	
	private static Keyspace searchKeyspace;
	
	private static String arrayHandler;
	
	private static Map<String,String> fieldsDataTypeCache;
	
	private static Map<String,Map<String,String>>  fieldsCustomDataTypeCache;
	
	private static Map<String,Map<String,String>> fieldsConfigCache;
	
	private static Map<String,Map<String,String>> fieldsCache;
	
	private static Map<String,Map<String,Map<String, String>>> dependentFieldsCache;
	
	private static Map<String,String> fieldArrayHandler;
	
	private static Map<String,String> indexMap;
	
	private ColumnList<String> endPoint = null;
	
	protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
	 
	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BaseCassandraService baseCassandraService;
	
	@Resource(name="cassandra")
	Properties cassandra;
	
	@Autowired
	RedisService redisService;
	
	public Properties getCassandraProperties() {
		return cassandra;
	}
	
	@PostConstruct
	public void initConnect(){
		
		if(!baseAPIService.checkNull(insightsKeyspace) || !baseAPIService.checkNull(searchKeyspace)){
		initCassandraConnection();
		
		}

		if(!baseAPIService.checkNull(devClient)){
			initDevESConnection();
		}
		
		if(!baseAPIService.checkNull(prodClient)){
			initProdESConnection();
		}

		if(!baseAPIService.checkNull(indexMap)){
			indexList();
		}

		if(!baseAPIService.checkNull(dependentFieldsCache)){
			dependentFields();
		}
		
		if(!baseAPIService.checkNull(fieldsDataTypeCache)){
			fieldDataType();
		}
		
		if(!baseAPIService.checkNull(fieldsConfigCache) || !baseAPIService.checkNull(fieldsCache) ||  !baseAPIService.checkNull(fieldsCustomDataTypeCache)){
			fieldsConfig();
		}
		
		if(!baseAPIService.checkNull(fieldArrayHandler)){
			fieldArrayHandler();
		}
	}
	
	public Client getDevClient(){
	
		if(!baseAPIService.checkNull(devClient)){
			initDevESConnection();
		}
		
		return devClient;
	}
	
	public Client getProdClient(){
		
		if(!baseAPIService.checkNull(prodClient)){
			initProdESConnection();
		}
		
		return prodClient;
	}
	
	public void initCassandraConnection(){
	
		 String seeds = this.getCassandraProperties().getProperty(cassandraConfigs.SEEDS.cassandraConfig());
         Integer port = Integer.parseInt(this.getCassandraProperties().getProperty(cassandraConfigs.PORT.cassandraConfig()));
         String clusterName = this.getCassandraProperties().getProperty(cassandraConfigs.CLUSTER.cassandraConfig());
         String insights = this.getCassandraProperties().getProperty(cassandraConfigs.INSIGHTS_KEYSPACE.cassandraConfig());
         String search = this.getCassandraProperties().getProperty(cassandraConfigs.SEARCH_KEYSPACE.cassandraConfig());

		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster(clusterName)
        .forKeyspace(insights)
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
        .withConnectionPoolConfiguration(connectionConfig(seeds,port))
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		insightsKeyspace = (Keyspace)context.getClient();
		
		context = new AstyanaxContext.Builder()
        .forCluster(clusterName)
        .forKeyspace(search)
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.NONE)
        .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
        .withConnectionPoolConfiguration(connectionConfig(seeds,port))
        .withConnectionPoolMonitor(new Slf4jConnectionPoolMonitorImpl())
        .buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		searchKeyspace = context.getClient();
		System.out.println("insights keyspace "+ insightsKeyspace +" and search keyspace "+searchKeyspace);
		
	}
	
	public ConnectionPoolConfigurationImpl connectionConfig(String seeds,Integer port){
		StringBuffer seedConfigured = new StringBuffer();
		for(String seed : seeds.split(",")){
			if(seedConfigured.length() > 0){
				seedConfigured.append(",");
			}
			seedConfigured.append(seed+":"+port);
		}
		
		System.out.println("seeds"+seedConfigured.toString());
		ConnectionPoolConfigurationImpl connectionPoolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
		.setPort(port.intValue())
		.setMaxConnsPerHost(2)
		.setSeeds(seedConfigured.toString());
		
		if (!seeds.startsWith("127.0")) {
			connectionPoolConfig.setLocalDatacenter("datacenter1");
		}
		
//		connectionPoolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range
		return connectionPoolConfig;
	}
	
	public void initDevESConnection(){
		OperationResult<ColumnList<String>> rowResult =baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),esConfigs.DEV_ROWKEY.esConfig(), new ArrayList<String>());
		ColumnList<String> columnList = rowResult.getResult();
		String clusterName = columnList.getStringValue(esConfigs.CLUSTER.esConfig(),"") ;
		System.out.println(" cluster "+clusterName);

		String hostName = columnList.getColumnByName(esConfigs.HOSTS.esConfig()).getStringValue();
		System.out.println(" hostname "+hostName);

		String portNo = columnList.getColumnByName(esConfigs.PORTNO.esConfig()).getStringValue();
		System.out.println(" portNo "+portNo);

		String nodeType = columnList.getColumnByName(esConfigs.NODE.esConfig()).getStringValue();
		System.out.println(" nodeType "+nodeType);

		if(nodeType != null && !nodeType.isEmpty()){
		if(esConfigs.NODE_CLIENT.esConfig().equalsIgnoreCase(nodeType)){
			devClient  = initNodeClient(clusterName);
		}
		}
		if(!baseAPIService.checkNull(devClient)){
			devClient = initTransportClient(hostName,portNo,clusterName);
		}
	}

	public void initProdESConnection(){
		OperationResult<ColumnList<String>> rowResult =baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),esConfigs.ROWKEY.esConfig(), new ArrayList<String>());
		ColumnList<String> columnList = rowResult.getResult();
		String clusterName = columnList.getStringValue(esConfigs.CLUSTER.esConfig(),"") ;
		System.out.println(" cluster "+clusterName);

		String hostName = columnList.getColumnByName(esConfigs.HOSTS.esConfig()).getStringValue();
		System.out.println(" hostname "+hostName);

		String portNo = columnList.getColumnByName(esConfigs.PORTNO.esConfig()).getStringValue();
		System.out.println(" portNo "+portNo);

		String nodeType = columnList.getColumnByName(esConfigs.NODE.esConfig()).getStringValue();
		System.out.println(" nodeType "+nodeType);

		if(nodeType != null && !nodeType.isEmpty()){
		if(esConfigs.NODE_CLIENT.esConfig().equalsIgnoreCase(nodeType)){
			prodClient  = initNodeClient(clusterName);
		}
		}
		if(!baseAPIService.checkNull(prodClient)){
			prodClient = initTransportClient(hostName,portNo,clusterName);
		}
	}
	
	
	public Client initNodeClient(String clusterName){
		 Settings settings = ImmutableSettings.settingsBuilder().put(esConfigs.ES_CLUSTER.esConfig(), clusterName != null ? clusterName : "").put("client.transport.sniff", true).build();
		 return new NodeBuilder().settings(settings).node().client();   	
	}
	
	public Client initTransportClient(String hostName,String portNo,String clusterName){
		 Settings settings = ImmutableSettings.settingsBuilder().put(esConfigs.ES_CLUSTER.esConfig(), clusterName != null ? clusterName : "").put("client.transport.sniff", true).build();
         TransportClient transportClient = new TransportClient(settings);
         transportClient.addTransportAddress(new InetSocketTransportAddress(hostName, Integer.valueOf(portNo)));
         return transportClient;
	}
	
	public Keyspace connectInsights(){
		
		if(!baseAPIService.checkNull(insightsKeyspace)){
			initCassandraConnection();
		}
		return insightsKeyspace;
	}
	
	public Keyspace connectSearch(){
		
		if(!baseAPIService.checkNull(searchKeyspace)){
			initCassandraConnection();
		}
		return searchKeyspace;
	}
	
	public void fieldDataType(){
		
		fieldsDataTypeCache = new HashMap<String, String>();
		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(keyspaces.INSIGHTS.keyspace(), columnFamilies.EVENT_FIELDS.columnFamily(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		
		for(Row<String, String> row : rows){
			fieldsDataTypeCache.put(row.getKey(),row.getColumns().getStringValue("description",row.getKey()));
		}
	}
	
	public Set<String> indexList(){
		indexMap = new HashMap<String,String>();
		Set<String> fetchFields = new HashSet<String>(); 

		OperationResult<ColumnList<String>> rowResult =baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),esConfigs.ES_INDICES.esConfig(), new ArrayList<String>());
		for(Column<String> column :  rowResult.getResult()){
			indexMap.put(column.getName(), column.getStringValue());
			fetchFields.add(column.getStringValue());
		}
		System.out.println("key set"+ indexMap);
		return fetchFields;
	}
	
	public void fieldsConfig(){

		fieldsConfigCache = new HashMap<String, Map<String,String>>();
		fieldsCache = new HashMap<String,Map<String,String>>();
		fieldsCustomDataTypeCache = new HashMap<String,Map<String,String>>();

		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),indexList(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		
		for(Row<String,String> row : rows){
			Map<String,String> configMap = new HashMap<String, String>();
			Map<String,String> fieldsMap = new HashMap<String, String>();
			Map<String,String> fieldsDataTypeMap = new HashMap<String, String>();
			if(row.getColumns().getColumnByName("common_fields") != null && row.getColumns().getColumnByName("fetch_fields") != null){
			configMap.put(row.getColumns().getColumnByName("common_fields").getStringValue(), row.getColumns().getColumnByName("fetch_fields").getStringValue());
			}
			for(Column<String> column : row.getColumns()){
				fieldsMap.put(column.getName(),column.getStringValue());
			}
			OperationResult<ColumnList<String>> result = baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),row.getKey()+"~datatype",new ArrayList<String>());
			
			for(Column<String> column : result.getResult()){
				fieldsDataTypeMap.put(column.getName(),column.getStringValue());
			}
			fieldsCustomDataTypeCache.put(row.getKey(), fieldsDataTypeMap);
			fieldsCache.put(row.getKey(),fieldsMap);
			fieldsConfigCache.put(row.getKey(), configMap);
		}
	}

	public void dependentFields(){
		
		dependentFieldsCache = new HashMap<String,Map<String,Map<String, String>>>();
		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),indexList(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		for(Row<String,String> row : rows){
			Map<String,Map<String,String>> dataSet = new HashMap<String, Map<String,String>>();
			if(row.getColumns().getColumnByName("dependent_fields") != null){
			 String dependentKeys = row.getColumns().getColumnByName("dependent_fields").getStringValue();
			 Set<String> fetchFields = new HashSet<String>(); 
				for(String dependentKey : dependentKeys.split(",")){
					fetchFields.add(row.getKey()+"~"+dependentKey);
				}
				operationalResult =  baseCassandraService.readAll(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(), fetchFields, new ArrayList<String>());
				Rows<String,String> dependentrows = operationalResult.getResult();
				for(Row<String,String> dependentRow : dependentrows){
					Map<String,String> dependentMap = new HashMap<String, String>();
					for(Column<String> column : dependentRow.getColumns()){
						dependentMap.put(column.getName(),column.getStringValue());
					}
					String[] key = dependentRow.getKey().split("~");
					dataSet.put(key[1], dependentMap);
				}
				dependentFieldsCache.put(row.getKey(), dataSet);
			}
		}
		System.out.println(" dependent list "+dependentFieldsCache);
	}
	
	public void fieldArrayHandler(){
		fieldArrayHandler = new HashMap<String, String>();
		arrayHandler = new String();
		ColumnList<String> operationalResult = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONFIG_SETTINGS.columnFamily(),cassRowKeys.FILED_ARRAY_HANDLER.cassRowKey());
		Collection<String> columnList = operationalResult.getColumnNames();
		arrayHandler = baseAPIService.convertCollectiontoString(columnList);
		for(Column<String> column : operationalResult){
			fieldArrayHandler.put(column.getName(),column.getStringValue());
		}
	}
	
	public Map<String,Map<String,Map<String, String>>> getDependentFieldsCache() {
		
		if(!baseAPIService.checkNull(dependentFieldsCache)){
			dependentFields();
		}
		return dependentFieldsCache;
	}

	public Map<String,Map<String,String>> getFields() {
		
		if(!baseAPIService.checkNull(fieldsCache)){
			fieldsConfig();
		}
		return fieldsCache;
	}
	
	public Map<String, String> getFieldsDataType() {
		
		if(!baseAPIService.checkNull(fieldsDataTypeCache)){
			fieldDataType();
		}
		return fieldsDataTypeCache;
	}

	public Map<String, Map<String, String>> getFieldsJoinCache() {
		
		if(!baseAPIService.checkNull(fieldsConfigCache)){
			fieldsConfig();
		}
		return fieldsConfigCache;
	}
	
	public Map<String, Map<String, String>>  getFieldsCustomDataType() {
		
		if(!baseAPIService.checkNull(fieldsCustomDataTypeCache)){
			fieldsConfig();
		}
		return fieldsCustomDataTypeCache;
	}
	
	public Map<String, String> getIndexMap() {
	
		if(!baseAPIService.checkNull(indexMap)){
		
			indexList();
		}
		return indexMap;
	}
	
	public boolean clearDataCache(){
		try{
		fieldsDataTypeCache = new HashMap<String, String>();
		indexMap = new HashMap<String,String>();
		fieldsConfigCache = new HashMap<String, Map<String,String>>();
		fieldsCache = new HashMap<String,Map<String,String>>();
		fieldsCustomDataTypeCache = new HashMap<String,Map<String,String>>();
		dependentFieldsCache = new HashMap<String,Map<String,Map<String, String>>>();
		fieldArrayHandler = new HashMap<String, String>();
		arrayHandler = new String();
		return true;
		}catch(Exception e){
			return false;
	}
	}
	
	public void clearConnectionCache(){
		insightsKeyspace = null;
		searchKeyspace = null;
		devClient = null;
		prodClient = null;
	}
	
	public Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap){
		endPoint = baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),"gooru.api.rest.endpoint", new ArrayList<String>()).getResult();
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
				userMap.put("emailId",jsonObj.getString("emailId"));
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
	
	public Map<String,Object> getUserObjectData(String sessionToken ,Map<Integer,String> errorMap){	

			String result = redisService.getRedisRawValue(GOORU_PREFIX+sessionToken);
			Map<String,Object> userMap = new LinkedHashMap<String, Object>();
			try{
				JSONObject coreJsonObject = new JSONObject(result);
				Set<String> permissionSet = new HashSet<String>();
				Map<String,Set<String>> partyPermissions = new HashMap<String, Set<String>>();

				JSONObject jsonObject = new JSONObject(coreJsonObject.getString("userToken"));
				jsonObject = new JSONObject(jsonObject.getString("user"));
				userMap.put("firstName",jsonObject.getString("firstName"));
				userMap.put("lastName",jsonObject.getString("lastName"));
				userMap.put("emailId",jsonObject.getString("emailId"));
				userMap.put("gooruUId",jsonObject.getString("partyUid"));
				userMap.put("userRoleSetString",jsonObject.getString("userRoleSetString"));
				
				JSONObject userCredential = new JSONObject(coreJsonObject.getString("userCredential"));
				JSONObject partyPermissionList = new JSONObject(userCredential.getString("partyPermissions"));
				Iterator<String> keys= partyPermissionList.keys();

				while(keys.hasNext()){
					String key = keys.next();
					JSONArray jsonArray = partyPermissionList.getJSONArray(key);
					for(int i=0;i < jsonArray.length();i++){
						permissionSet.add(jsonArray.getString(i));
					}
					partyPermissions.put(key, permissionSet);
				}
				System.out.println("party Map "+partyPermissions);
				userMap.put("permissions", partyPermissions);
			}catch(Exception e){
				errorMap.put(500, e.toString());
			}
		return userMap;
	
	}

	public String getArrayHandler() {
		
		if(arrayHandler == null || arrayHandler.isEmpty()){
			fieldArrayHandler();
		}
		return arrayHandler;
	}

	public  Map<String, String> getFieldArrayHandler() {
		
		if(fieldArrayHandler == null || fieldArrayHandler.isEmpty()){
			fieldArrayHandler();
		}
		return fieldArrayHandler;
	}

}

