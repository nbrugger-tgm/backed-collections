package com.niton.memory.direct.stores;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.niton.memory.direct.DataStore;

public class FileStore extends DataStore {
	private final RandomAccessFile file;

	public FileStore(RandomAccessFile file) {
		this.file = file;
	}
	public FileStore(File f) throws FileNotFoundException {
		this.file = new RandomAccessFile(f,"rw");
	}

	@Override
	public long size() {
		try {
			return file.length();
		} catch (IOException e) {
			return -1;
		}
	}

	@Override
	protected byte[] innerRead(long from, long to) {
		try {
			byte[] res = new byte[(int) (to-from)];
			file.seek(from);
			file.read(res,0, (int) (to-from));
			jump(file.getFilePointer());
			return res;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void innerWrite(byte[] data, long from, long to) {
		try {
			file.seek(from);
			file.write(data,0, (int) (to-from));
			jump(file.getFilePointer());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long cut(long from) {
		try {
			long sz= size();
			file.setLength(from);
			jump(from);
			return (sz-from);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
