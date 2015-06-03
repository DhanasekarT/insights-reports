package org.gooru.insights.services;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CSVFileWriterService {

	void generateCSVReport(String traceId, Set<String> headerKeys, List<Map<String, Object>> rowList, String fileAbsolutePath, String delimiter, Boolean isNewFile) throws FileNotFoundException;

	void removeExpiredFile();

}
