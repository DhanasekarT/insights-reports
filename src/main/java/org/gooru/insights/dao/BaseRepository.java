package org.gooru.insights.dao;

import java.util.List;
import java.util.Map;

public interface BaseRepository {

	public Object[] getAnswerByQuestionId(String questionId) ;
	public List<Map<String, String>> getAnswerDataList(String questionId) ;
	public List<Object[]> getAnswerByAnswerId(String questionId) ;
	public String getHintText(long hintId) ;
}
