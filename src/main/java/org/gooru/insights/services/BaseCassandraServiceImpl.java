package org.gooru.insights.services;

import java.util.Collection;

import org.gooru.insights.constants.CassandraConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;

@Service
public class BaseCassandraServiceImpl implements BaseCassandraService,CassandraConstants{

	@Autowired
	BaseConnectionService connector;
	
	@Autowired
	BaseAPIService baseAPIService;
	
	public OperationResult<ColumnList<String>> readColumns(String keyspace, String columnFamilyName, String rowKey,Collection<String> columns) {

			Keyspace queryKeyspace = null;
			if (keyspaces.INSIGHTS.keyspace().equalsIgnoreCase(keyspace)) {
				queryKeyspace = connector.connectInsights();
			} else {
				queryKeyspace = connector.connectSearch();
			}
			try {
				RowQuery<String, String> rowQuery = queryKeyspace.prepareQuery(this.accessColumnFamily(columnFamilyName)).getKey(rowKey);
			
				if(baseAPIService.checkNull(columns)){
					rowQuery.withColumnSlice(columns);
				}
				return rowQuery.execute();
			} catch (ConnectionException e) {

				e.printStackTrace();
				System.out.println("Query execution exeption");
			}
			return null;

		}
	
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> columnFamily;

		columnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return columnFamily;
	}
}

