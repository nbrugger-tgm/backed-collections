package com.niton.collections.backed.stores;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileStoreTest extends DataStoreTest {
	@Override
	protected DataStore createDataStoreImpl() {
			try {
			File f = new File("ram.dat");
			if(!f.exists())
				f.createNewFile();
			System.out.println("Testfile: "+f.getAbsolutePath());
			FileStore fs= new FileStore(f);
			return fs;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
