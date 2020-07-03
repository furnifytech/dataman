package com.furnify.dataman.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

public class FileReader {

	private FileReader() {  }

	public static File readXML(Path xmlPath) throws IOException {
		File xmlFile = xmlPath.toFile();
		if(!xmlFile.exists())
			throw new FileNotFoundException("File not found in the path - " + xmlPath);
		if(!xmlFile.canRead())
			throw new SecurityException("Unable to read file - " + xmlPath);
		return xmlPath.toFile();
	}
	
}
