package com.furnify.dataman.processor;

import com.furnify.dataman.model.Table;
import com.furnify.dataman.model.Tables;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataUpdater {
	
	private static final Logger logger = Logger.getLogger(DataUpdater.class.getName());

	public static boolean importFromXML(Connection con, Path xmlPath) {
		try {
	        JAXBContext jaxbContext = JAXBContext.newInstance(Tables.class);
	        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
	        Tables tables = (Tables) unmarshaller.unmarshal(xmlPath.toFile());
	        for(Table table : tables.getTable()) {
                logger.log(Level.INFO, "Starting to Process : " + table.getName() + " at " + LocalDateTime.now());
	        	TableImporter.processTable(con, table);
                logger.log(Level.INFO, "Completed Processing " + table.getName() + " at " + LocalDateTime.now());
	        }
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Exception Occurred in XML Import", e);
		}
		return true;
	}

	public static void importFromDirectory(Connection con, String directory) throws IOException {
		Path dirPath = Paths.get(directory).toAbsolutePath().normalize();

		List<Path> xmlFileList = Files.list(dirPath)
				.filter(path -> Files.isReadable(path))
				.collect(Collectors.toList());

		xmlFileList.forEach(path -> importFromXML(con, path));
	}
	
}
