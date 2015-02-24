package org.gooru.insights.services;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

public interface ExcelBuilderService {

	String generateExcelReport(String startDate,String endDate,Integer partnerIpdId,List<Map<String,String>> resultSet,String FileName)throws ParseException, IOException;

	String exportXlsReport(List<Map<String,Object>> listOfMap,String fileName,boolean isNewFile)throws ParseException, IOException;
	
}
