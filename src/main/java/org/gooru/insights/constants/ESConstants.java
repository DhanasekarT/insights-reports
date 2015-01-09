package org.gooru.insights.constants;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.migration.avro.IndexType;
import org.apache.commons.collections.map.HashedMap;
import org.apache.lucene.codecs.sep.IntIndexInput.Index;

public interface ESConstants {

	public enum esConfigs{
		ROWKEY("es~connection"),DEV_ROWKEY("es~connection~dev"),ES_INDICES("es~indices"),INDEX("index_name"),CLUSTER("cluster_name"),ES_CLUSTER("cluster.name"),NODE("node_type"),HOSTS("host_names"),NODE_CLIENT("node_type"),TRANSPORT_CLIENT("transportClient"),PORTNO("port_no");
		
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
	
}
