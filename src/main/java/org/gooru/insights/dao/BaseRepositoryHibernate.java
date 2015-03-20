package org.gooru.insights.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional
public class BaseRepositoryHibernate implements BaseRepository {
	@Autowired
	public SessionFactory sessionFactory;

	@Override
	public Object[] getAnswerByQuestionId(String questionId) {
		String sql = "select c.gooru_oid as answer_gooru_oid, answer_text from content c inner join assessment_answer a on a.question_id = c.content_id where is_correct = 1 and c.gooru_oid ='" + questionId + "' limit 1";
		return (Object[]) sessionFactory.getCurrentSession().createSQLQuery(sql).list().get(0);
	}
	
	@Override
	public List<Map<String, String>> getAnswerDataList(String questionId) {
		String sql = "select c.gooru_oid as answer_gooru_oid, answer_text from content c inner join assessment_answer a on a.question_id = c.content_id where is_correct = 1 and c.gooru_oid ='" + questionId + "' ";
		SQLQuery result = sessionFactory.getCurrentSession().createSQLQuery(sql);
		List<Map<String, String>> answerList = new ArrayList<Map<String, String>>();
		Map<String, String> value = new HashMap<String, String>();
		List<Object[]> results = result.list();
		for(Object[] object :results){
			value.put("answer_gooru_oid" , object[0].toString());
			value.put("answer_text" , object[1].toString());
			answerList.add(value);
		}
		return answerList;
	}
	
	@Override
	public List<Object[]> getAnswerByAnswerId(String questionId) {
		String sql = "select c.gooru_oid as answer_gooru_oid, answer_text from content c inner join assessment_answer a on a.answer_id = c.content_id where is_correct = 1 and c.gooru_oid ='" + questionId + "' limit 1";
		return sessionFactory.getCurrentSession().createSQLQuery(sql).list();
	}
	
}
