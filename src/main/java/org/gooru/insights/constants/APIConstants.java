package org.gooru.insights.constants;

public interface APIConstants {

	public static String CACHE_PREFIX ="insights";
	
	public static String CACHE_PREFIX_ID ="key";
	
	public static String SEPARATOR="~";
	
	public enum hasdata{
		HAS_FEILDS("hasFields"),HAS_DATASOURCE("hasDataSource"),HAS_GRANULARITY("hasGranularity"),HAS_GROUPBY("hasGroupBy"),HAS_INTERVALS("hasIntervals"),
		HAS_FILTER("hasFilter"),HAS_AGGREGATE("hasAggregate"),HAS_PAGINATION("hasPagination"),HAS_LIMIT("hasLimit"),HAS_Offset("hasOffset"),HAS_SORTBY("hasSortBy"),HAS_SORTORDER("hasSortOrder");
		
		private String name;
		
		public String check(){
		return name;	
		}
		
		private hasdata(String name){
			this.name = name;
		}
	}
}
