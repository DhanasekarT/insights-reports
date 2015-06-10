package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

public interface ExcelWriterService {

	void generateExcelReport(String traceId, List<String> headerKeys, List<Map<String, Object>> rowList, String fileAbsolutePath, boolean isNewFile);

}
