package org.ednovo.gooru.cassandra;


import java.io.IOException;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Repository
public class CassandraConnectionProvider {

    private Keyspace cassandraKeyspace;
    private Keyspace cassandraAwsKeyspace;
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);
    private static String CASSANDRA_IP;
    private static String CASSANDRA_PORT;
    private static String CASSANDRA_KEYSPACE;
    public void init(Map<String, String> configOptionsMap) {

        CASSANDRA_IP = "198.199.93.161:9160";
//    	CASSANDRA_IP = "127.0.0.1:9160";
        CASSANDRA_PORT = "9160";
        CASSANDRA_KEYSPACE = "insights_qa";

        String esClusterName = "";
        String esHost = "107.170.199.76";
        int esPort = 9300;
        try {

            logger.info("Loading cassandra properties");
            String hosts = CASSANDRA_IP;
            String keyspace = CASSANDRA_KEYSPACE;

            String clusterName = "Test Cluster";
            
            //MyConnectionPool
            ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("myCPConfig")
                    .setPort(9160)
                    .setMaxConnsPerHost(3)
                    .setSeeds(hosts);
            if (!hosts.startsWith("127.0")) {
                poolConfig.setLocalDatacenter("datacenter1");
            }

            poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range

            AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                    .forCluster(clusterName)
                    .forKeyspace(keyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                    .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
                    .withConnectionPoolConfiguration(poolConfig)
                    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            context.start();

            cassandraKeyspace = (Keyspace) context.getClient();
            logger.info("Initialized connection to Cassandra");
            if(cassandraAwsKeyspace == null ){
            	cassandraAwsKeyspace = this.initializeAwsCassandra();
            }
        } catch (Exception e) {
            logger.info("Error while initializing cassandra", e);
        }
    }

    public  Keyspace initializeAwsCassandra(){
		 
		String awsNewHosts =  "54.219.62.215";
//		String awsNewHosts =  "127.0.0.1:9160";
		String awsNewCluster = "gooru-cassandra-qa";
//		String awsNewCluster = "Test Cluster";
		String keyspace = CASSANDRA_KEYSPACE;
//		String keyspace = "gooru_local";
		ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
	    .setPort(9160)
	    .setMaxConnsPerHost(3)
	    .setSeeds(awsNewHosts);
		
		if (!awsNewHosts.startsWith("127.0")) {
//			poolConfig.setLocalDatacenter("us-west");
			poolConfig.setLocalDatacenter("datacenter1");
		}
	
		//poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range
	
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
		    .forCluster(awsNewCluster)
		    .forKeyspace(keyspace)
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
		    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		    .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
		    .withConnectionPoolConfiguration(poolConfig)
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());
	
		context.start();
		
		cassandraAwsKeyspace = (Keyspace) context.getClient();
		
        logger.info("Initialized connection to AWS Cassandra");
        
		return cassandraAwsKeyspace;
        
	}
    
    public Keyspace getKeyspace() throws IOException {
    	if (cassandraKeyspace == null) {
    		this.init(null);
        }
        return cassandraKeyspace;
    }
    public Keyspace getAwsKeyspace() throws IOException {
        if (cassandraAwsKeyspace == null) {
            throw new IOException("New Keyspace not initialized.");
        }
        return cassandraAwsKeyspace;
    }
}
