package org.gooru.insights.constants;

public interface APIConstants {

	public static String CACHE_PREFIX ="insights";
	
	public static String DI_REPORTS ="di_reports";
	
	public static String CACHE_PREFIX_ID ="key";
	
	public static String GOORU_PREFIX = "authenticate_";
	
	public static String SEPARATOR="~";
	
	public static String WILD_CARD="*";
	
	public static String AP_SELF_ACTIVITY = "AP_SELF_ACTIVITY";
	
	public static String AP_SELF_PII = "AP_SELF_PII";
	
	public static String AP_PARTY_PUBLIC = "AP_PARTY_PUBLIC";
	
	public static String AP_PARTY_PII = "AP_PARTY_PII";
	
	public static String AP_ALL_PARTY_ALL_DATA = "AP_ALL_PARTY_ALL_DATA";
	
	public static String AP_PARTY_ALL_DATA = "AP_PARTY_ALL_DATA";
	
	public static String AP_SYSTEM_PUBLIC = "AP_SYSTEM_PUBLIC";
	
	public static String AP_OWN_CONTENT_USAGE = "AP_OWN_CONTENT_USAGE";
	
	public static String AP_PARTY_OWN_CONTENT_USAGE = "AP_PARTY_OWN_CONTENT_USAGE";
	
	public static String AP_PARTY_ACTIVITY = "AP_PARTY_ACTIVITY";
	
	public static String AP_APP_SESSION_PARTY_ACTIVITY = "AP_APP_SESSION_PARTY_ACTIVITY";
	
	public static String AP_APP_SESSION_PARTY_CONTENT_USAGE = "AP_APP_SESSION_PARTY_CONTENT_USAGE";
	
	public static String AP_PARTY_ACTIVITY_RAW = "AP_PARTY_ACTIVITY_RAW";
	
	public static String CONTENTORGUID = "contentOrganizationUId";
	
	public static String USERORGID = "userOrganizationUId";
	
	public static String GOORUUID = "gooruUId";
	
	public static String USERUID = "userUid";
	
	public static String CREATORUID = "creatorUid";
	
	public static String CONTENT_ORG_UID = "content_organization_uid";
	
	public static String USER_ORG_UID = "user_organization_uid";
	
	public static String GOORU_UID = "gooru_uid";
	
	public static String CREATOR_UID = "creator_uid";
	
	public static String ACTIVITY = "rawData";
	
	public static String CONTENT = "content";
	
	public static String USER = "userdata";
	
	public static String DEFAULTORGUID = "4261739e-ccae-11e1-adfb-5404a609bd14";

	public String ACTIVITYDATASOURCES =  ".*rawData.*|.*rawdata.*|.*activity.*|.*Activity.*";
	
	public String CONTENTDATASOURCES =  ".*content.*|.*resource.*";
	
	public String RESTRICTEDPERMISSION =  ".*AP_PARTY_ACTIVITY_RAW.*|.*AP_PARTY_PII.*|.*AP_ALL_PARTY_ALL_DATA.*|.*AP_PARTY_ALL_DATA.*";

	public String USERDATASOURCES =  ".*userData.*|.*userdata.*|.*user.*|.*User.*";
	
	public String USERFILTERPARAM =  ".*user_uid.*|.*userUid.*|.*gooru_uid.*|.*gooruUId.*|.*creatorUid.*|.*creator_uid.*";
	
	public String ORGFILTERPARAM =  ".*contentOrganizationUId.*|.*userOrganizationUId.*|.*content_organization_uid.*|.*user_organization_uid.*|.*organizationUId.*|.*contentOrganizationUid.*|.*userOrganizationUid.*";
	
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
