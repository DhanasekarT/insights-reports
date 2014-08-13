package org.gooru.insights.services;

import java.util.Collection;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;

public interface BaseCassandraService {

	OperationResult<ColumnList<String>> readColumns(String keyspace, String columnFamilyName, String rowKey,Collection<String> columns);
}
