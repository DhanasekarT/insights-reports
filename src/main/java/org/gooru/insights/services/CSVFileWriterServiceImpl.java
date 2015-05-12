package org.gooru.insights.services;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.gooru.insights.constants.APIConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CSVFileWriterServiceImpl implements CSVFileWriterService{
	
	@Autowired
	private BaseConnectionService baseConnectionService;
	
	private static final Logger logger = LoggerFactory.getLogger(CSVFileWriterServiceImpl.class);
	private static final String repoPath = "/home/e100068/gooru/data";
	private static final String fileDir = "/insights-reports/";
	private String defaultDelimiter = "|";
	private String newLine = "";
	
	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}
	
	/*@Override
	public String generateCSVReport(List<Map<String, Object>> rowList, String fileName, String delimiter, Boolean isNewFile) throws FileNotFoundException {
		
		String absoluteFileName = repoPath + fileDir + fileName;
		
		if(delimiter == null || delimiter.isEmpty()) {
			delimiter = defaultDelimiter;
		}
		
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(absoluteFileName), true)));
		
		if (isNewFile) {
			// print header row
			int loopCount = 0 ;
			for (Map.Entry<String, Object> entry : rowList.get(0).entrySet()) {
				stream.print(loopCount == 0 ? BaseConnectionServiceImpl.exportFieldCache.get(entry.getKey()) : delimiter + BaseConnectionServiceImpl.exportFieldCache.get(entry.getKey()));
				loopCount++;
			}
			// print new line
			stream.println(newLine);
		}
		
		StringBuilder rowLine = new StringBuilder(); 
		for (Map<String, Object> map : rowList) {
			
			// print row values
			int loopCount = 0;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				rowLine = (loopCount == 0 ? rowLine.append(entry.getValue()) : rowLine.append(delimiter + entry.getValue()));
				loopCount++;
			}
			stream.print(rowLine);
			rowLine.delete(0, rowLine.length() - 1);
			
			// print new line
			stream.println(newLine);
			stream.flush();
		}
		logger.info("Added {} rows in file. Filepath : {}", rowList.size(), absoluteFileName );
		return fileDir + fileName;
	
	}*/
	
	@Override
	public void generateCSVReport(List<Map<String, Object>> rowList, String fileAbsolutePath, String delimiter, Boolean isNewFile) throws FileNotFoundException {
		
//		String absoluteFileName = repoPath + fileDir + fileName;
		
		if(delimiter == null || delimiter.isEmpty()) {
			delimiter = APIConstants.PIPE;
		}
		
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(fileAbsolutePath), true)));
		
		if (isNewFile) {
			// print header row
			int loopCount = 0 ;
			for (Map.Entry<String, Object> entry : rowList.get(0).entrySet()) {
				stream.print(loopCount == 0 ? getBaseConnectionService().getExportReportCache().get(entry.getKey()) : delimiter + getBaseConnectionService().getExportReportCache().get(entry.getKey()));
				loopCount++;
			}
			// print new line
			stream.println(APIConstants.EMPTY);
		}
		
		StringBuilder rowLine = new StringBuilder(); 
		for (Map<String, Object> map : rowList) {
			
			// print row values
			int loopCount = 0;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				rowLine = (loopCount == 0 ? rowLine.append(entry.getValue()) : rowLine.append(delimiter + entry.getValue()));
				loopCount++;
			}
			stream.print(rowLine);
			rowLine.delete(0, rowLine.length() - 1);
			
			// print new line
			stream.println(newLine);
			stream.flush();
		}
		logger.debug("Added {} rows in file. Filepath : {}", rowList.size(), fileAbsolutePath );
//		return fileDir + fileName;
	}
}
