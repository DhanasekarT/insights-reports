package org.gooru.insights.services;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.gooru.insights.constants.ESConstants.esConfigs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;


public class XAPIExporter {

	List<Map<String, Object>> exportActivity(SearchResponse searchResponse) {
		List<Map<String, Object>> activityList = new ArrayList<Map<String, Object>>();
		SearchHit[] hits = searchResponse.getHits().getHits();
		
		for (SearchHit searchHit : hits) {
			Map<String, Object> activityAsMap = new HashMap<String, Object>(); 
			
			System.out.println(searchHit.getSource());
			Map<String, Object> source = searchHit.getSource();
			activityAsMap.put("id",source.get("event_id"));
			
			if (source.containsKey("gooru_uid") && StringUtils.isNotBlank(source.get("gooru_uid").toString())) {
				Map<String, Object> actorAsMap = new HashMap<String, Object>(1);

				actorAsMap.put("objectType", "Agent");
				actorAsMap.put("id", source.get("gooru_uid"));
				actorAsMap.put("apiKey", source.get("api_key"));
				actorAsMap.put("organizationUid", source.get("user_organization_uid"));
				actorAsMap.put("userIp", source.get("user_ip"));
				actorAsMap.put("userAgent", source.get("user_agent"));

				activityAsMap.put("actor", actorAsMap);

			}
			if (source.containsKey("content_gooru_id") && StringUtils.isNotBlank(source.get("content_gooru_id").toString())) {
				Map<String, Object> objectAsMap = new HashMap<String, Object>(1);
				String objectType = null;
				if (source.containsKey("type_name") && StringUtils.isNotBlank(source.get("type_name").toString())) {
					if (source.get("type_name").toString().equalsIgnoreCase("assessment-question")) {
						objectType = "Resource";
					} else {
						objectType = "Activity";
					}
				}
				objectAsMap.put("objectType", objectType);

				objectAsMap.put("id", source.get("content_gooru_id"));
				objectAsMap.put("apiKey", source.get("api_key"));
				objectAsMap.put("organizationUid", source.get("user_organization_uid"));
				objectAsMap.put("userIp", source.get("user_ip"));
				objectAsMap.put("userAgent", source.get("user_agent"));

				activityAsMap.put("actor", objectAsMap);

			}
			if(source.containsKey("event_name") && StringUtils.isNotBlank(source.get("event_name").toString())) {
				Map<String, String> verbAsMap = new HashMap<String, String>();
				String verb = null;
				if(source.get("event_name").toString().equalsIgnoreCase("item.create") && source.get("mode").toString().equalsIgnoreCase("copy")) {
					verb = "copied";
				} else if(source.get("event_name").toString().equalsIgnoreCase("item.create") && source.get("mode").toString().equalsIgnoreCase("copy")) {
					verb = "moved";
				} else if(source.get("event_name").toString().equalsIgnoreCase("item.create")) {
					verb = "created";
				} else if(source.get("event_name").toString().endsWith("play") && source.get("mode").toString().equalsIgnoreCase("study")) {
					verb = "studied";
				}
				verbAsMap.put("id", "www.goorulearning.org/exapi/verbs/"+verb);
				activityAsMap.put("verb", verbAsMap);
			}
			if(source.containsKey("parent_gooru_id") && StringUtils.isNotBlank(source.get("parent_gooru_id").toString())) {
				List<Map<String, Map<String, Object>>> contextActivitiesList = new ArrayList<Map<String, Map<String, Object>>>();
				Map<String, Map<String, Object>> contextAsMap = new HashMap<String, Map<String, Object>>();
				Map<String, Object> parentAsMap = new HashMap<String, Object>(1);
				parentAsMap.put("id", source.get("parent_gooru_id").toString());
				if(source.containsKey("parent_event_id") && StringUtils.isNotBlank(source.get("parent_event_id").toString())){
					parentAsMap.put("id", source.get("parent_event_id").toString());
					parentAsMap.put("objectType", "Activity");
				}
				contextAsMap.put("parent", parentAsMap);
				contextActivitiesList.add(contextAsMap);
				
			}
			if(source.containsKey("total_timespent_ms") && StringUtils.isNotBlank(source.get("total_timespent_ms").toString())) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				resultMap.put("duration", source.get("total_timespent_ms"));
				if(source.containsKey("score") && StringUtils.isNotBlank(source.get("score").toString())) {
					Map<String, Object> scaledAsMap = new HashMap<String, Object>(1);
					scaledAsMap.put("score", Long.valueOf(source.get("score").toString()));
					resultMap.put("scaled", scaledAsMap);
				}
				activityAsMap.put("result", resultMap);
			}

			
			activityAsMap.put("timestamp", source.get("event_time"));
			activityAsMap.put("stored", source.get("event_time"));
			

			System.out.println("activityID : " +source.get("event_id"));
			
			/*
			 * Map<String, Object> responseAsMap = (Map<String, Object>) mapper.readValue(searchHit.getSource(), new TypeReference<Map<String,
			 * Object>>() { });
			 */
			activityList.add(activityAsMap);
		}
		return activityList;
	}

	List<Map<String, Object>> exportActivity(JSONArray activityArray) throws JSONException {
		List<Map<String, Object>> activityList = new ArrayList<Map<String, Object>>();

		for (int index = 0; index < activityArray.length(); index++) {
			Map<String, Object> activityAsMap = new HashMap<String, Object>();
			JSONObject activityJsonObject = activityArray.getJSONObject(index);
			activityAsMap.put("id", activityJsonObject.get("eventId"));

			if (activityJsonObject.isNull("gooruUId") && StringUtils.isNotBlank(activityJsonObject.get("gooruUId").toString())) {
				Map<String, Object> actorAsMap = new HashMap<String, Object>(1);
				actorAsMap.put("objectType", "Agent");
				actorAsMap.put("id", activityJsonObject.get("gooruUId"));
				actorAsMap.put("apiKey", activityJsonObject.get("apiKey"));
				actorAsMap.put("organizationUid", activityJsonObject.get("userOrganizationUId"));
				if (activityJsonObject.isNull("userIp") && StringUtils.isNotBlank(activityJsonObject.get("userIp").toString())) {
					actorAsMap.put("userIp", activityJsonObject.get("userIp"));
					actorAsMap.put("userAgent", activityJsonObject.get("userAgent"));
				}
				activityAsMap.put("actor", actorAsMap);
			}
			if (activityJsonObject.isNull("gooruOid") && StringUtils.isNotBlank(activityJsonObject.get("gooruOid").toString())) {
				Map<String, Object> objectAsMap = new HashMap<String, Object>(1);
				String objectType = null;
				if (activityJsonObject.isNull("typeName") && StringUtils.isNotBlank(activityJsonObject.get("typeName").toString())) {
					if (activityJsonObject.get("typeName").toString().equalsIgnoreCase("assessment-question")) {
						objectType = "Resource";
					} else {
						objectType = "Activity";
					}
				}
				objectAsMap.put("objectType", objectType);
				objectAsMap.put("id", activityJsonObject.get("gooruOid"));
				activityAsMap.put("object", objectAsMap);

			}
			if (activityJsonObject.isNull("eventName") && StringUtils.isNotBlank(activityJsonObject.get("eventName").toString())) {
				Map<String, String> verbAsMap = new HashMap<String, String>();
				String verb = null;
				if (activityJsonObject.get("eventName").toString().equalsIgnoreCase("item.create") && activityJsonObject.get("mode").toString().equalsIgnoreCase("copy")) {
					verb = "copied";
				} else if (activityJsonObject.get("eventName").toString().equalsIgnoreCase("item.create") && activityJsonObject.get("mode").toString().equalsIgnoreCase("copy")) {
					verb = "moved";
				} else if (activityJsonObject.get("eventName").toString().equalsIgnoreCase("item.create")) {
					verb = "created";
				} else if (activityJsonObject.get("eventName").toString().endsWith("play")) {
					verb = "studied";
				} else if (activityJsonObject.get("eventName").toString().endsWith("reaction")) {
					verb = "reacted";
				} else if (activityJsonObject.get("eventName").toString().endsWith("view")) {
					verb = "viewed";
				} else if (activityJsonObject.get("eventName").toString().endsWith("edit")) {
					verb = "edited";
				} else if (activityJsonObject.get("eventName").toString().endsWith("delete")) {
					verb = "deleted";
				} else if (activityJsonObject.get("eventName").toString().endsWith("rate") || activityJsonObject.get("eventName").toString().endsWith("review")) {
					verb = "reviewed";
				} else if (activityJsonObject.get("eventName").toString().endsWith("comment")) {
					verb = "commented";
				}
				verbAsMap.put("id", "www.goorulearning.org/exapi/verbs/" + verb);
				activityAsMap.put("verb", verbAsMap);
			}
			if (activityJsonObject.isNull("parentGooruId") && StringUtils.isNotBlank(activityJsonObject.get("parentGooruId").toString())) {
				List<Map<String, Map<String, Object>>> contextActivitiesList = new ArrayList<Map<String, Map<String, Object>>>();
				Map<String, Map<String, Object>> contextAsMap = new HashMap<String, Map<String, Object>>();
				Map<String, Object> parentAsMap = new HashMap<String, Object>(1);
				parentAsMap.put("id", activityJsonObject.get("parentGooruId").toString());
				if (activityJsonObject.isNull("parentEventId") && StringUtils.isNotBlank(activityJsonObject.get("parentEventId").toString())) {
					parentAsMap.put("id", activityJsonObject.get("parentEventId").toString());
					parentAsMap.put("objectType", "Activity");
				}
				contextAsMap.put("parent", parentAsMap);
				contextActivitiesList.add(contextAsMap);

			}
			if (activityJsonObject.isNull("totalTimeSpentInMs") && StringUtils.isNotBlank(activityJsonObject.get("totalTimeSpentInMs").toString())) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				resultMap.put("duration", activityJsonObject.get("totalTimeSpentInMs"));
				if (activityJsonObject.isNull("score") && StringUtils.isNotBlank(activityJsonObject.get("score").toString())) {
					Map<String, Object> scaledAsMap = new HashMap<String, Object>(1);
					scaledAsMap.put("score", Long.valueOf(activityJsonObject.get("score").toString()));
					resultMap.put("scaled", scaledAsMap);
				}
				activityAsMap.put("result", resultMap);
			}

			activityAsMap.put("timestamp", activityJsonObject.get("eventTime"));
			activityAsMap.put("stored", activityJsonObject.get("eventTime"));

			System.out.println("activityID : " + activityJsonObject.get("eventId"));

			/*
			 * Map<String, Object> responseAsMap = (Map<String, Object>) mapper.readValue(searchHit.getSource(), new TypeReference<Map<String,
			 * Object>>() { });
			 */
			activityList.add(activityAsMap);
		}
		return activityList;
	}
	public static void main(String a[]) throws JSONException, IOException, ParseException {/*
		XAPIExporter xAPIExporter = new XAPIExporter();
		

		Client client = null;
		Settings settings = ImmutableSettings.settingsBuilder().put(esConfigs.ES_CLUSTER.esConfig(), "insights_cluster").put("client.transport.sniff", true).build();
		TransportClient transportClient = new TransportClient(settings);
		transportClient.addTransportAddress(new InetSocketTransportAddress("54.177.68.49", 9300));
		client = transportClient;
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("event_logger_info_20141231").setTypes("event_detail").setQuery(QueryBuilders.matchAllQuery()).setFrom(1).setSize(3);
		SearchResponse searchResponse = (SearchResponse) searchRequestBuilder.execute().actionGet();
		System.out.println(xAPIExporter.exportActivity(searchResponse));

		ClientResource clientResource = null;
		Representation representation = null;
		JsonRepresentation jsonRepresentation;
		try {
			String uri = "http%3A%2F%2Fwww.goorulearning.org%2Finsights%2Fapi%2Fv2%2Fquery%3FsessionToken%3D89d61af6-7b14-11e4-8d16-123141016e2a%26data%3D%7B%2522fields%2522%3A%2522%2522%2C%2522dataSource%2522%3A%2522rawData%2522%2C%2522granularity%2522%3A%2522%2522%2C%2522filter%2522%3A%5B%7B%2522logicalOperatorPrefix%2522%3A%2522AND%2522%2C%2522fields%2522%3A%5B%7B%2522type%2522%3A%2522selector%2522%2C%2522valueType%2522%3A%2522String%2522%2C%2522fieldName%2522%3A%2522course%2522%2C%2522operator%2522%3A%2522eq%2522%2C%2522value%2522%3A%252220670%2522%7D%5D%7D%5D%2C%2522aggregations%2522%3A%5B%5D%2C%2522groupBy%2522%3A%2522%2522%2C%2522pagination%2522%3A%7B%2522offset%2522%3A0%2C%2522limit%2522%3A1%2C%2522order%2522%3A%5B%7B%7D%5D%7D%7D";
			clientResource = new ClientResource(uri);
			if (clientResource.getStatus().isSuccess()) {
				representation = clientResource.get();
				jsonRepresentation = new JsonRepresentation(representation.getText());
				if (jsonRepresentation != null) {
					try {
						JSONArray activityArray = jsonRepresentation.getJsonObject().getJSONArray("content");
						for (int index = 0; index < activityArray.length(); index++) {
							JSONObject activityJsonObject = activityArray.getJSONObject(index);
							System.out.println(activityJsonObject.get("eventName"));
						}
						System.out.println(xAPIExporter.exportActivity(activityArray));
						ObjectMapper objectMapper = new ObjectMapper();
						Map<String, Object> responseAsMap = (Map<String, Object>) objectMapper.readValue(activityArray.toString(),
                                new TypeReference<Map<String, Object>>() {
                                });

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			try {
				if (clientResource != null) {
					clientResource.release();
				}
				if (representation != null) {
					representation.release();
				}
				clientResource = null;
				representation = null;
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

	
		
		//System.out.println(new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH).format(System.currentTimeMillis()));

		SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" );
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

		Date start = formatter.parse( "2014-06-12T19:22:35.645Z" );
		//DateTime stop = formatter.parseDateTime( "2014-06-12T19:22:35.645Z" );
		int i = 700000000;
		
		Period period = new Period(i);

			
		System.out.println( "start: " + start.toString() );
		System.out.println( "timestamp: " + start.getTime() );
		System.out.println( "period: " + new Period((long)Long.valueOf(700000000)));
		System.out.println();
	*/
		int attemptCount = 6;
		int[] attempStatus = {0,0,0,1,0,1};
		int status = 0;
		status = Integer.parseInt(attemptCount+"");
		if(status != 0){
			status = status-1;
		}
		int attemptStatus = attempStatus[status];
		System.out.println(attemptStatus);
		
		int[] attemptStatus1 = new int[]{};
		System.out.println(attemptStatus1.length);
		System.out.println(attemptStatus1[0]);
		
		//JSONObject attemptJson = JsonSerializer.serialize(attempts); 
		}
}
