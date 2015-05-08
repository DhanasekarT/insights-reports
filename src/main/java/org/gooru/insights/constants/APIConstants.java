package org.gooru.insights.constants;

public class APIConstants {
	
	/**
	 * Symbols
	 */
	public static final String COMMA = ",";
	
	public static final String COLON = ":";
	
	public static final String DOT = ".";
	
	public static final String EMPTY= "";
	
	public static final String PIPE = "|";

	public static final String EMPTY_JSON_ARRAY = "[{}]";
	
	public static final String SEPARATOR = "~";
	
	public static final String WILD_CARD = "*";
	
	public static final String OPEN_BRACE = "{";
	
	public static final String CLOSE_BRACE = "}";
	
	public static final String F = "f";
	
	public static final String P = "p";
	
	/**
	 * view Attributes
	 */
	public static final String VIEW_NAME = "VIEW_NAME";
	
	public static final String RESPONSE_NAME = "RESPONSE_NAME";
	
	/**
	 * Date formats
	 */
	public static final String DEFAULT_FORMAT = "yyyy-MM-dd kk:mm:ss";

	/**
	 * Environment constants
	 */
	public static final String DEV = "dev";
	
	public static final String PROD = "prod";
	
	/**
	 * cache constants
	 */
	public static final String CACHE_PREFIX = "insights";
	
	public static final String CACHE_PREFIX_ID = "key";
	
	public static final String GOORU_PREFIX = "authenticate_";
	
	/**
	 * Authorization constants
	 */
	public static final String AP_SELF_ACTIVITY = "AP_SELF_ACTIVITY";
	
	public static final String AP_SELF_PII = "AP_SELF_PII";
	
	public static final String AP_PARTY_PUBLIC = "AP_PARTY_PUBLIC";
	
	public static final String AP_PARTY_PII = "AP_PARTY_PII";
	
	public static final String AP_ALL_PARTY_ALL_DATA = "AP_ALL_PARTY_ALL_DATA";
	
	public static final String AP_PARTY_ALL_DATA = "AP_PARTY_ALL_DATA";
	
	public static final String AP_SYSTEM_PUBLIC = "AP_SYSTEM_PUBLIC";
	
	public static final String AP_OWN_CONTENT_USAGE = "AP_OWN_CONTENT_USAGE";
	
	public static final String AP_PARTY_OWN_CONTENT_USAGE = "AP_PARTY_OWN_CONTENT_USAGE";
	
	public static final String AP_PARTY_ACTIVITY = "AP_PARTY_ACTIVITY";
	
	public static final String AP_APP_SESSION_PARTY_ACTIVITY = "AP_APP_SESSION_PARTY_ACTIVITY";
	
	public static final String AP_APP_SESSION_PARTY_CONTENT_USAGE = "AP_APP_SESSION_PARTY_CONTENT_USAGE";
	
	public static final String AP_PARTY_ACTIVITY_RAW = "AP_PARTY_ACTIVITY_RAW";
	
	public static final String DI_REPORTS = "di_reports";
	
	public static final String ANONYMOUS_USERDATA_SOURCE = "anonymoususerdata";
	
	/**
	 * parameter constants
	 */
	public static final String ORG_FILTERS = "orgFilters";
	
	public static final String USER_FILTERS = "userFilters";
	
	public static final String LIMIT = "limit";
	
	public static final String OFFSET = "offset";
	
	public static final String SORT_ORDER = "sortOrder";
	
	public static final String _CODEID = "code_id";
	
	public static final String IN = "in";

	public static final String OR = "OR";

	public static final String LABEL = "label";

	public static final String DEPTH = "depth";
	
	public static final String CONSTANT_VALUE = "constant_value";
	
	public static final String GOORU_SESSION_TOKEN = "Gooru-Session-Token";
	
	public static final String SESSION_TOKEN = "sessionToken";
	
	public static final String URL = "url";
	
	public static final String TRACE_ID = "traceId";
	
	public static final String GRANULARITY_NAME = "granularity";
	
	public static final String DATA_SOURCE = "dataSource";
	
	public static final String GROUP_BY_DATA_SOURCE = "groupByDataSource";
	
	public static final String FIELDS = "fields";
	
	public static final String FILTER_FIELDS = "filterFields";
	
	public static final String FILTER_DATA_SOURCE = "filterDataSource";
	
	public static final String GROUP_BY = "groupBy";
	
	public static final String SELECTOR = "selector";
	
	public static final String AGGREGATE_ATTRIBUTE = "aggregateAttribute";
	
	public static final String RANGE_ATTRIBUTE = "rangeAttribute";
	
	public static final String MULTIPLE_GROUPBY = "multipleGroupby";
	
	public static final String LOGICAL_OPERATOR = "logicalOperator";
	
	public static final String FILTERS = "filterAtributes";
	
	public static final String SORT_BY = "sortBy";

	public static final String DESC = "DESC";
	
	public static final String PERMISSIONS = "permissions";
	
	public static final String FIELD_NAME = "field_name";
	
	public static final String DEPENDENT_NAME = "dependent_name";
	
	public static final String USER_TOKEN = "userToken";
	
	public static final String PARTY_UID = "partyUid";
	
	public static final String USER_ROLE_SETSTRING = "userRoleSetString";
	
	public static final String USER_CREDENTIAL = "userCredential";
	
	public static final String USER = "user";
	
	public static final String PARTY_PERMISSIONS = "partyPermissions";

	public static final String FIRST_NAME = "firstName";
	
	public static final String LAST_NAME = "lastName";
	
	public static final String EMAIL_ID = "emailId";
	
	public static final String GRANULARITY[] = {"YEAR","MONTH","QUATOR","WEEK","DAY","HOUR","MINUTE","SECOND","H","M","S","D","W","M","Y","year","month","quator","week","day","hour","minute","second"};
	
	public static final String QUERY = "query";
	
	public static final String QUERYS = "query's";
	
	public static final String CONTENTORGUID = "contentOrganizationUId";
	
	public static final String USERORGID = "userOrganizationUId";
	
	public static final String GOORUUID = "gooruUId";
	 
	public static final String USERUID = "userUid";
	
	public static final String CREATORUID = "creatorUid";
	
	public static final String CONTENT_ORG_UID = "content_organization_uid";
	
	public static final String USER_ORG_UID = "user_organization_uid";

	public static final String GOORU_UID = "gooru_uid";
	
	public static final String CREATOR_UID = "creator_uid";
	
	public static final String ACTIVITY = "rawData";
	
	public static final String CONTENT = "content";
	
	public static final String USER_DATA = "userdata";
	
	public static final String DEFAULTORGUID = "4261739e-ccae-11e1-adfb-5404a609bd14";

	public static final String ACTIVITYDATASOURCES =  ".*rawData.*|.*rawdata.*|.*activity.*|.*Activity.*";
	
	public static final String CONTENTDATASOURCES =  ".*content.*|.*resource.*";
	
	public static final String RESTRICTEDPERMISSION =  ".*AP_PARTY_ACTIVITY_RAW.*|.*AP_PARTY_PII.*|.*AP_ALL_PARTY_ALL_DATA.*|.*AP_PARTY_ALL_DATA.*";

	public static final String USERDATASOURCES =  ".*userData.*|.*userdata.*|.*user.*|.*User.*";
	
	public static final String USERFILTERPARAM =  ".*user_uid.*|.*userUid.*|.*gooru_uid.*|.*gooruUId.*|.*creatorUid.*|.*creator_uid.*";
	
	public static final String ORGFILTERPARAM =  ".*contentOrganizationUId.*|.*userOrganizationUId.*|.*content_organization_uid.*|.*user_organization_uid.*|.*organizationUId.*|.*contentOrganizationUid.*|.*userOrganizationUid.*";
	
	/**
	 * Serializer Excludes
	 */
	public static final String EXCLUDE_CLASSES = "*.class";
	
	/**
	 * Message constants
	 */
	
	public static final String JSON_FORMAT = "JSON Format";
			
	public static final String GOORU_URI = "/v2/user/token/";
	
	public static final String PROCEED = "proceed";
	
	public static final String API_REQUEST = "doing API request";
	
	public static final String TOMCAT_STATUS = "Tomcat started";
	
	public static final String STATUS_NAME = "STATUS_NAME";
	
	public static final String PARTY_PERMISSION_MESSAGE = "party permissions:";
	
	public static final String OLD_QUERY = "old query:";
	
	public static final String NEW_QUERY = "new query:";

	public static final String DELETED = "DELETED";
	
	public static final String UPDATE = "UPDATE";
	
	public static final String ADD = "ADD";
	
	public static final String INSERTED = "INSERTED";

	public static final String NOT_FOUND = "NOT_FOUND";
	
	public static final String FAILED = "FAILED";
	
	public static final String STATUS = "STATUS";
	
	public static final String CACHE_CLEAR = "CACHE_CLEAR";
	
	public static final String DATA = "DATA";
	
	public static final String CONNECTION = "CONNECTION";
	
	public static final String ALLOWED_ORG = "allowedOrg";
	
	public static final String ROLES = "roles";
	
	public static final String QUERY_ID = "queryId";
	
	public static final String ASC = "ASC";
	
	public static final String TOKEN = "token";

	/**
	 * Logical Enumaration 
	 */
	public static enum Hasdatas {
		
		HAS_FEILDS("hasFields"), HAS_DATASOURCE("hasDataSource"), HAS_GRANULARITY("hasGranularity"), 
		HAS_GROUPBY("hasGroupBy"), HAS_INTERVALS("hasIntervals"), HAS_FILTER("hasFilter"), 
		HAS_AGGREGATE("hasAggregate"), HAS_PAGINATION("hasPagination"),HAS_MULTIGET("hasMultiGet"), HAS_LIMIT("hasLimit"), 
		HAS_Offset("hasOffset"), HAS_SORTBY("hasSortBy"), HAS_SORTORDER("hasSortOrder"),HAS_DATASOURCE_FILTER("hasDataSourceFilter"),HAS_RANGE("hasRange");

		private String name;

		public String check() {
			return name;
		}

		private Hasdatas(String name) {
			this.name = name;
		}
	}
	
	public static enum LogicalConstants {

		FIELD("field"),DATE("date");
		
		private String field;
		
		public String value() {
			return field;
		}
		
		private LogicalConstants(String name) {
			this.field = name;
		}
		
	}

	public static enum DateHistory { 
		
		D_CHECKER("(.*)D|(.*)d"), W_CHECKER("(.*)W|(.*)w"), H_CHECKER("(.*)H|(.*)h"),
		K_CHECKER("(.*)K|(.*)k"), S_CHECKER("(.*)S|(.*)s"), D_REPLACER("D|d"),
		W_REPLACER("W|w"), H_REPLACER("H|h"), K_REPLACER("K|k"),
		S_REPLACER("S|s");
		
		private String replacer;
		
		public String replace() {
			return replacer;
		}
		private DateHistory(String replacer) {
			this.replacer = replacer;
		}
	}
	
	public static enum DateFormats {

		DEFAULT("yyyy-MM-dd hh:kk:ss"), YEAR("yyyy"), QUARTER("yyyy-MM-dd"), MONTH("yyyy-MM"), 
		WEEK("yyyy-MM-dd"), DAY("yyyy-MM-dd"), HOUR("yyyy-MM-dd hh"), MINUTE("yyyy-MM-dd hh:kk"), 
		SECOND("yyyy-MM-dd hh:kk:ss"), D("yyyy-MM-dd"), W("yyyy-MM-dd"), 
		H("yyyy-MM-dd hh"), K("yyyy-MM-dd hh:kk"), S("yyyy-MM-dd hh:kk:ss"), MILLISECOND("yyyy-MM-dd hh:kk:ss.SSS"),  NANOSECOND("yyyy-MM-dd hh:kk:ss.SSS");

		private String format;

		public String format() {
			return format;
		}

		private DateFormats(String format) {
			this.format = format;
		}
	}
	
	public static enum FormulaFields {

		FORMULA("formula"), REQUEST_VALUES("requestValues"), METRICS("metrics"), 
		NAME("name"), AGGREGATIONS("aggregations"), FIELD("field"), 
		BUCKETS("buckets"), TOTAL_ROWS("totalRows"), KEY("key"), 
		DOC_COUNT("doc_count"), KEY_AS_STRING("key_as_string"), SOURCE("_source"), 
		HITS("hits"), TOTAL("total"), FIELDS("fields"),FROM("from"),TO("to"),FROM_AS_STRING("from_as_string"),TO_AS_STRING("to_as_string"), 
		FILTERS("filters"), VALUES("values"), VALUE("value");

		private String field;

		public String getField() {
			return field;
		}

		private FormulaFields(String field) {
			this.field = field;
		}
	}

	public static enum DataTypes {
	
		STRING("String"), LONG("Long"), INTEGER("Integer"), 
		DOUBLE("Double"), SHORT("Short"), DATE("Date");

		private String field;

		public String dataType() {
			return field;
		}

		private DataTypes(String field) {
			this.field = field;
		}
	}
	
	public static enum AggregateFields {

		SUM("sum"), AVG("avg"), MAX("max"),
		MIN("min"),COUNT("count"),DISTINCT("distinct"),
		PERCENTILES("percentiles"), PERCENTS("percents");
		
		private String field; 
		
		public String getField() {
			return field;
		}
		
		private AggregateFields(String field) {
			this.field = field;
		}
	}
	
	public static enum EsFilterFields {
		
		AND("AND"), OR("OR"), NOT("NOT"),
		GT("gt"), FILTERS("filters"), SELECTOR("selector"),
		RG("rg"), NRG("nrg"), EQ("eq"),
		LK("lk"), EX("ex"), IN("in"),
		LE("le"), GE("ge"), LT("lt");

		private String field;
		
		public String field() {
			return field;
		}
		
		private EsFilterFields(String field) {
			this.field = field;
		}
	}
	
	public static enum Numbers {
		FOUR("4"), FIVE("5");
		
		private String number;
		
		public String getNumber() {
			return number;
		}
		
		private Numbers(String number) {
			this.number = number;
		}
	}
}
