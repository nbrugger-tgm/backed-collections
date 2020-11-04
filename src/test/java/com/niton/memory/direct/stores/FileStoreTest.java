package com.niton.memory.direct.stores;

import com.niton.memory.direct.DataStore;

import java.io.File;
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
			throw new MemoryException(e);
		}
	}
}
