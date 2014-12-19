package org.ednovo.gooru.cassandra;

import java.io.IOException;


import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

public class BaseDAOCassandraImpl {

	protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
//	protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
	
    @Autowired
    private CassandraConnectionProvider connectionProvider;
    
    private Keyspace keyspace;
    
    private static final Logger logger = LoggerFactory.getLogger(BaseDAOCassandraImpl.class);
    
    
    public BaseDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public void setConectionProvider(CassandraConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }
    
    public Keyspace getKeyspace() {
        if(keyspace == null && this.connectionProvider != null) {
        	try {
                this.keyspace = this.connectionProvider.getKeyspace();
            } catch (IOException ex) {
                logger.info("Error while initializing keyspace{}", ex);
            }
        }
        return this.keyspace;
    }
}
