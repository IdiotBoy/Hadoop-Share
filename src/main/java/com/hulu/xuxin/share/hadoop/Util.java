package com.hulu.xuxin.share.hadoop;

import java.io.File;

public class Util {

	public static void removeExistingPath(String path) {
		File file = new File(path);
		if (file.exists())
			removeFiles(file);
	}
	
	private static void removeFiles(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles())
				removeFiles(f);
		}
		file.delete();
	}
}
