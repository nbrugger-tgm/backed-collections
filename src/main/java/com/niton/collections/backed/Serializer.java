package com.niton.collections.backed;

import com.niton.StorageException;
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
			BufferedOutputStream bufOut = new BufferedOutputStream(store,4);
			DataOutputStream dos = new DataOutputStream(bufOut);
			dos.writeInt(data);
			bufOut.flush();
		}

		@Override
		public Integer read(InputStream store) throws IOException {

			return new DataInputStream(new BufferedInputStream(store,4)).readInt();
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

	public byte[] serialize(T key)  {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			write(key,bos);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		return bos.toByteArray();
	}
}
