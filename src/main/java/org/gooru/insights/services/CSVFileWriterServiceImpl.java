package org.gooru.insights.services;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gooru.insights.constants.APIConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class CSVFileWriterServiceImpl implements CSVFileWriterService{
	
	@Autowired
	private BaseConnectionService baseConnectionService;
	
	private static final Logger logger = LoggerFactory.getLogger(CSVFileWriterServiceImpl.class);
	
	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}
	
	@Override
	public void generateCSVReport(Set<String> headerKeys, List<Map<String, Object>> rowList, String fileAbsolutePath, String delimiter, Boolean isNewFile) throws FileNotFoundException {
		
		if(delimiter == null || delimiter.isEmpty()) {
			delimiter = APIConstants.PIPE;
		}
		
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(fileAbsolutePath), true)));
		
		if (isNewFile) {
			// print header row
			Iterator<String> itr = headerKeys.iterator();
			String header = null;
			while(itr.hasNext()) {
				header = itr.next();
				if(getBaseConnectionService().getExportReportCache().containsKey(header)) {
					header = getBaseConnectionService().getExportReportCache().get(header);
				}
				stream.print(header);
				if(itr.hasNext()) {
					stream.print(delimiter);
				}
			}
			// print new line
			stream.println(APIConstants.EMPTY);
		}
		
		StringBuilder rowLine = new StringBuilder(); 
		for (Map<String, Object> row : rowList) {
			int loopCount = 0;
			for(String header : headerKeys) {
				String key = row.get(header) == null ? APIConstants.NOT_APPLICABLE: row.get(header).toString();
				rowLine = (loopCount == 0 ? rowLine.append(key) : rowLine.append(delimiter.concat(key)));
				loopCount++;
			}
			stream.print(rowLine);
			rowLine.delete(0, rowLine.length());
			
			// print new line
			stream.println(APIConstants.EMPTY);
			stream.flush();
		}
		logger.debug("Added {} rows in file. Filepath : {}", rowList.size(), fileAbsolutePath );
	}
}
