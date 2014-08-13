package org.gooru.insights.services;

import org.elasticsearch.client.Client;

import com.netflix.astyanax.Keyspace;

public interface BaseConnectionService {

	Keyspace connectInsights();
	
	Keyspace connectSearch();
	
	Client getClient();
}
