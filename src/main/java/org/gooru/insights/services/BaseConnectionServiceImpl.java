package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.CassandraConstants.keyspaces;
import org.gooru.insights.constants.ESConstants.esConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Component
public class BaseConnectionServiceImpl implements BaseConnectionService,CassandraConstants {

	private static Client client;
	
	private static Keyspace insightsKeyspace;
	
	private static Keyspace searchKeyspace;
	
	private static Map<String,String> fieldsCache;
	
	 protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
	 
	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BaseCassandraService baseCassandraService;
	
	@Resource(name="cassandra")
	Properties cassandra;
	
	public Properties getCassandraProperties() {
		return cassandra;
	}
	
	@PostConstruct
	public void initConnect(){
		
		if(insightsKeyspace == null || searchKeyspace == null){
		initCassandraConnection();
		if( fieldsCache == null ){
			eventFields();
		}
		}
		
		if(client == null ){
		initESConnection();
		}
		
	}
	
	public Client getClient(){
	
		return this.client;
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
        .setDiscoveryType(NodeDiscoveryType.NONE)
        .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
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
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
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
		.setMaxConnsPerHost(30)
		.setSeeds(seedConfigured.toString());
		
		if (!seeds.startsWith("127.0")) {
			connectionPoolConfig.setLocalDatacenter("datacenter1");
		}
		
		connectionPoolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range
		return connectionPoolConfig;
	}
	
	public void initESConnection(){
		OperationResult<ColumnList<String>> rowResult =baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.CONNECTION_CONFIG_SETTING.columnFamily(),esConfigs.ROWKEY.esConfig(), new ArrayList<String>());
		ColumnList<String> columnList = rowResult.getResult();
		String indexName = columnList.getColumnByName(esConfigs.INDEX.esConfig()).getStringValue();
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
			client  = initNodeClient(clusterName);
		}
		}
		if(client == null){
			client = initTransportClient(hostName,portNo,clusterName,indexName);
		}
	}
	
	public Client initNodeClient(String clusterName){
		 Settings settings = ImmutableSettings.settingsBuilder().put(esConfigs.ES_CLUSTER.esConfig(), clusterName != null ? clusterName : "").put("client.transport.sniff", true).build();
		 return new NodeBuilder().settings(settings).node().client();   	
	}
	
	public Client initTransportClient(String hostName,String portNo,String clusterName,String indexName){
		 Settings settings = ImmutableSettings.settingsBuilder().put(esConfigs.ES_CLUSTER.esConfig(), clusterName != null ? clusterName : "").put("client.transport.sniff", true).build();
         TransportClient transportClient = new TransportClient(settings);
         transportClient.addTransportAddress(new InetSocketTransportAddress(hostName, Integer.valueOf(portNo)));
         return transportClient;
	}
	
	public Keyspace connectInsights(){
		return insightsKeyspace;
	}
	
	public Keyspace connectSearch(){
		return searchKeyspace;
	}


	public void eventFields(){
		OperationResult<Rows<String, String>> operationalResult = baseCassandraService.readAll(keyspaces.INSIGHTS.keyspace(), columnFamilies.EVENT_FIELDS.columnFamily(),new ArrayList<String>());
		Rows<String, String> rows = operationalResult.getResult();
		fieldsCache = new HashMap<String, String>();
		for(Row<String, String> row : rows){
			fieldsCache.put(row.getKey(),row.getColumns().getStringValue("be_column",row.getKey())) ; 
		}
		System.out.println("fields"+fieldsCache);
	}
	public Map<String, String> getFields() {
		return fieldsCache;
	}
}

