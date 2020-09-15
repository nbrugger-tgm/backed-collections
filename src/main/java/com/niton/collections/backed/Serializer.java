package com.niton.collections.backed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class Serializer<T> {
	private DataStore dataStore;
	public abstract void write(T data, OutputStream store) throws IOException;

	public abstract T read(InputStream store) throws IOException, ClassNotFoundException;

	public DataStore getDataStore() {
		return dataStore;
	}

	public void setDataStore(DataStore dataStore) {
		this.dataStore = dataStore;
	}
}
