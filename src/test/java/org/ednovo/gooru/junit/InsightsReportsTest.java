package org.ednovo.gooru.junit;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.ednovo.gooru.cassandra.BaseCassandraRepo;
import org.ednovo.gooru.cassandra.Constants;
import org.ednovo.gooru.table.ColumnFamily;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.netflix.astyanax.model.ColumnList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;

@TestExecutionListeners({DependencyInjectionTestExecutionListener.class})
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:applicationContext.xml")
public class InsightsReportsTest implements Constants {

	private String ENDPOINT = "http://www.goorulearning.org/insights/";
	
	private String PATH = "api/v2/query";

	private String TOKEN = "419c6c56-5295-11e4-8d6c-123141016e2a";
	
	private String QUERYIDS;
	
	@Autowired
	private BaseCassandraRepo baseCassandraRepo;
	
	public BaseCassandraRepo getBaseCassandraRepo(){
		return baseCassandraRepo;
	}
	
	@Before
	public void setUp(){
		ColumnList<String> queryUidFromCassandra = getBaseCassandraRepo().readWithKey(ColumnFamily.JOBCONFIGSETTING.getColumnFamily(), JUNITKEY);
		
		if(!StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITQUERYIDSCOLUMN).getName()) && !StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITQUERYIDSCOLUMN).getStringValue())){
			QUERYIDS = queryUidFromCassandra.getColumnByName(JUNITQUERYIDSCOLUMN).getStringValue();
		}
		if(!StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITENDPOINTCOLUMN).getName()) && !StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITENDPOINTCOLUMN).getStringValue())){
			ENDPOINT = queryUidFromCassandra.getColumnByName(JUNITENDPOINTCOLUMN).getStringValue();
		}
		if(!StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITPATHCOLUMN).getName()) && !StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITPATHCOLUMN).getStringValue())){
			PATH = queryUidFromCassandra.getColumnByName(JUNITPATHCOLUMN).getStringValue();
		} 
		if(!StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITTOKENCOLUMN).getName()) && !StringUtils.isBlank(queryUidFromCassandra.getColumnByName(JUNITTOKENCOLUMN).getStringValue())){
			TOKEN = queryUidFromCassandra.getColumnByName(JUNITTOKENCOLUMN).getStringValue();
		}
	}
	
	@Test
	public void checkInsightsReportsApi(){
		String query = null;
		String keyColumnInMap = "key";
		String result = null;
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		if(QUERYIDS.length() > 0){
			for(String queryId : QUERYIDS.split(",")){
				ColumnList<String> testQueryData = getBaseCassandraRepo().readWithKey(ColumnFamily.JUNITTESTQUERIES.getColumnFamily(), queryId);
				if(testQueryData.size() > 0){
					if(testQueryData.getColumnNames().contains(JUNITQUERYCOLUMN)){
						Map<String, Object> junitResult = new HashMap<String, Object>();
						query = testQueryData.getColumnByName(JUNITQUERYCOLUMN).getStringValue();
						Client client = Client.create(new DefaultClientConfig());
						WebResource webResource = client.resource(ENDPOINT);
						ClientResponse clientResponse = webResource.path(PATH).queryParam("sessionToken", TOKEN).queryParam("data", query).get(ClientResponse.class);
						result = clientResponse.getEntity(String.class);
						junitResult.put(keyColumnInMap, queryId);
						junitResult.put("result", result);
						junitResult.put("http_status_code", clientResponse.getStatus());
						junitResult.put("last_modified_on", new Date().getTime());
						resultList.add(junitResult);
					}
				}
				getBaseCassandraRepo().saveListOfResourceObject(ColumnFamily.JUNITTESTQUERIES.getColumnFamily(), keyColumnInMap, resultList);
			}
		}
	}
}
