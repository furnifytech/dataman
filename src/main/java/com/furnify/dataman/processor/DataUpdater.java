package com.furnify.dataman.processor;

import com.furnify.dataman.model.Table;
import com.furnify.dataman.model.Tables;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataUpdater {
	
	private static final Logger logger = Logger.getLogger(DataUpdater.class.getName());

	private DataUpdater() { }

	public static boolean importFromXML(Connection con, Path xmlPath, String profile) {
		try {
	        JAXBContext jaxbContext = JAXBContext.newInstance(Tables.class);
	        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
	        Tables tables = (Tables) unmarshaller.unmarshal(xmlPath.toFile());

			processTables(con, profile, tables);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Exception Occurred in XML Import", e);
		}
		return true;
	}
	public static boolean importFromXML(Connection con, InputStream inputStream, String profile) {
		try {
	        JAXBContext jaxbContext = JAXBContext.newInstance(Tables.class);
	        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
	        Tables tables = (Tables) unmarshaller.unmarshal(inputStream);

			processTables(con, profile, tables);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Exception Occurred in XML Import", e);
		}
		return true;
	}

	private static void processTables(Connection con, String profile, Tables tables) {
		TableImporter tableImporter = new TableImporter(con, profile);

		for(Table table : tables.getTable()) {
			if (logger.isLoggable(Level.INFO)) {
				logger.log(Level.INFO, String.format("Starting to Process : %s at %s", table.getName(), LocalDateTime.now()));
			}
			tableImporter.processTable(table);
			if (logger.isLoggable(Level.INFO)) {
				logger.log(Level.INFO, String.format("Completed Processing : %s at %s", table.getName(), LocalDateTime.now()));
			}
		}
	}

	public static void importFromDirectory(Connection con, String directory, String profile) throws IOException {
		Path dirPath = Paths.get(directory).toAbsolutePath().normalize();

		try (Stream<Path> fileList = Files.list(dirPath)) {
			List<Path> xmlFileList = fileList.filter(Files::isReadable)
					.collect(Collectors.toList());

			xmlFileList.forEach(path -> importFromXML(con, path, profile));
		}
	}
	
}
