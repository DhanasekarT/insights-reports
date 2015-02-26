package org.gooru.insights.constants;

import java.util.HashMap;
import java.util.Map;

public class DataUtils {

	private static Map<String, Integer> reactionValue;
	
	static {
		reactionValue = new HashMap<String, Integer>();
		reactionValue.put("i-need-help", 1);
		reactionValue.put("i-donot-understand", 2);
		reactionValue.put("meh", 3);
		reactionValue.put("i-can-understand", 4);
		reactionValue.put("i-can-explain", 5);
	}

	public static Integer getReactionAsInt(String key) {
		return reactionValue.get(key) == null ? 0 : reactionValue.get(key);
	}

}
