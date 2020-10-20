package com.niton.collections.backed;

import java.io.*;

public class OOSSerializer<T> extends Serializer<T> {
	@Override
	public void write(T data, OutputStream store) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(store);
		oos.writeObject(data);
		oos.close();
	}

	@Override
	public T read(InputStream store) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(store);
		T data = (T) ois.readObject();
		ois.close();
		return data;
	}
}
