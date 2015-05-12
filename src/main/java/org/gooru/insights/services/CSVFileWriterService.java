package org.gooru.insights.services;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

public interface CSVFileWriterService {

	void generateCSVReport(List<Map<String, Object>> rowList, String fileName, String delimiter, Boolean isNewFile) throws FileNotFoundException;

}
