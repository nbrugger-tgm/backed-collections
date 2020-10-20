package com.niton.collections.backed;

import com.niton.memory.direct.DataStore;

import java.io.*;

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

	public static final Serializer<String> STRING = new Serializer<>() {
		@Override
		public void write(String data, OutputStream store) throws IOException {
			DataOutputStream dos = new DataOutputStream(store);
			dos.writeBoolean(data != null);
			if(data != null)
				dos.writeUTF(data);
		}

		@Override
		public String read(InputStream store) throws IOException, ClassNotFoundException {

			DataInputStream dis =  new DataInputStream(store);
			if(dis.readBoolean())
				return dis.readUTF();
			else return null;
		}
	};
	public static final Serializer<Integer> INT = new Serializer<>() {
		@Override
		public void write(Integer data, OutputStream store) throws IOException {
			DataOutputStream dos = new DataOutputStream(store);
			dos.writeInt(data);
		}

		@Override
		public Integer read(InputStream store) throws IOException, ClassNotFoundException {
			return new DataInputStream(store).readInt();
		}
	};
	public static final Serializer<Byte> BYTE = new Serializer<>() {
		@Override
		public void write(Byte data, OutputStream store) throws IOException {
			DataOutputStream dos = new DataOutputStream(store);
			dos.writeByte(data);
		}

		@Override
		public Byte read(InputStream store) throws IOException, ClassNotFoundException {
			return new DataInputStream(store).readByte();
		}
	};
	public static final Serializer<Object> OBJECT = new OOSSerializer<>();
}
