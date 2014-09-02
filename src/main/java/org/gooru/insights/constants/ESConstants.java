package org.gooru.insights.constants;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;

public interface ESConstants {

	public enum esConfigs{
		ROWKEY("es~connection"),INDEX("index_name"),CLUSTER("cluster_name"),ES_CLUSTER("cluster.name"),NODE("node_type"),HOSTS("host_names"),NODE_CLIENT("nodeClient"),TRANSPORT_CLIENT("transportClient"),PORTNO("port_no");
		
		private String property;
		esConfigs(String name){
		this.property = name;	
		}
		public String esConfig(){
			return property;
		}
	}
	
	public enum esSources{
		SOURCE("_source"),FIELDS("fields");
		
		private String property;
		esSources(String name){
		this.property = name;	
		}
		public String esSource(){
			return property;
		}
	}
	
	public static String[] ALL_INDICES = {"event_logger","content_catalog","taxonomy_catalog","user_catalog"};
	
	public enum esIndices{
		RAW_DATA("rawdata");
		
		private String index;
		esIndices(String name){
		this.index = name;
		}
		public String esIndex(){
			return index;
		}
	}
	
	public enum esTypes{
		EVENT_DETAIL("event_detail");
		
		private String type;
		esTypes(String name){
		this.type = name;
		}
		public String esType(){
			return type;
		}
	}
}
