package org.gooru.insights.services;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

public interface CSVBuilderService {
	
	String generateCSV(String startDate,String endDate,Integer partnerIpdId,List<Map<String,String>> resultSet,String FileName)throws ParseException, IOException;

	File generateCSVReport(List<Map<String,Object>> resultSet,String fileName)throws ParseException, IOException;
	
	String generateCSVMapReport(List<Map<String,Object>> resultSet,String fileName)throws ParseException, IOException;

	public void removeExpiredFiles();
}
