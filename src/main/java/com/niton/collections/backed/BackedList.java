package com.niton.collections.backed;

import com.niton.collections.backed.streams.CounterOutputStream;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractList;
import java.util.RandomAccess;

public class BackedList<T> extends AbstractList<T> implements RandomAccess{
	private transient DataOutputStream dos;
	private transient DataInputStream dis;
	private transient CounterOutputStream shiftStream;
	private transient DataOutputStream shiftDos;

	public BackedList(DataStore store) {
		this(store,new OOSSerializer<>());
	}

	private final DataStore store;
	private final Serializer<T> serializer;

	public BackedList(DataStore store, Serializer<T> serializer) {
		this.store = store;
		this.serializer = serializer;
		dos = new DataOutputStream(store.new DataStoreOutputStream());
		dis = new DataInputStream(store.new DataStoreInputStream());
		shiftStream = new CounterOutputStream(store.new ShiftingOutputStream());
		shiftDos = new DataOutputStream(shiftStream);
		writeSize(0);
	}

	@Override
	public T get(int index) {
		int address = readAddress(index);
		store.jump(address);
		try {
			return serializer.read(dis);
		} catch (Exception e) {
			throw new RuntimeException("Backing Exception",e);
		}
	}

	private int readAddress(int index) {
		int size = size();
		if(index>=size)
			throw new IndexOutOfBoundsException(index);
		try {
			//int is 4 bytes
			store.skip(4*index);
			return dis.readInt();
		} catch (IOException e) {
			throw new RuntimeException("Backing error",e);
		}
	}

	@Override
	public int size() {
		store.jump(0);
		try {
			return dis.readInt();
		} catch (IOException e) {
			throw new RuntimeException("Backing error",e);
		}
	}

	@Override
	public T remove(int index) {
		T toRemove = get(index);
		int address = readAddress(index);
		int sizeToRemove = 0;
		if(size()==index+1){
			store.jump(address);
		}else{
			int followUpAddress = readAddress(index+1);
			sizeToRemove = followUpAddress-address;
			store.jump(followUpAddress);
			store.shiftAll(-sizeToRemove);
		}
		store.cut();
		removeFromIndex(index);
		for (int i = index+1; i < size(); i++) {
			writeAddress(i, readAddress(i)-sizeToRemove);
		}
		return toRemove;
	}

	private void writeAddress(int index, int address) {
		store.jump(4+index*4);
		try {
			dos.writeInt(address);
		} catch (IOException e) {
			throw new RuntimeException("",e);
		}
	}

	private void removeFromIndex(int index) {
		writeSize(size()-1);
		store.jump((index+1)*4);
		store.shiftAll(4);
	}

	private void writeSize(int i) {
		store.jump(0);
		try {
			dos.writeInt(i);
		} catch (IOException e) {
			throw new RuntimeException("",e);
		}
	}

	@Override
	public void add(int index, T element) {
		if(size() > 0) {
			if(index == size())
				store.jump(store.size());
			else
				store.jump(readAddress(index));
		}else{
			store.jump(4);
		}
		try {
			shiftStream.resetCounter();
			serializer.write(element, shiftStream);
			store.jump(5);
			int bytes = shiftStream.getCounter(); // only executed line in between
			addIndex(index,4+(4*(size()+1))+bytes);
			for(int i = index+1;i<size();i++)
				writeAddress(i,readAddress(i)+bytes);
		} catch (IOException e) {
			throw new RuntimeException("Store exception",e);
		}
	}

	private void addIndex(int index, int address) {
		writeSize(size()+1);
		store.skip((index)*4);
		try {
			shiftDos.writeInt(address);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public T set(int index, T element) {
		if(index+1 == size()){
			store.jump(readAddress(index));
			try {
				serializer.write(element,dos);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return element;
		}
		int nextAddress,address;
		int oldSize = readAddress(nextAddress = index+1)-(address = readAddress(index));
		store.jump(nextAddress);
		store.shiftAll(-oldSize);
		store.cut();
		store.jump(address);
		shiftStream.resetCounter();
		try {
			serializer.write(element,shiftStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		int written = shiftStream.getCounter();
		int diff = written-oldSize;
		for (int i = index+1; i < size(); i++) {
			writeAddress(i,readAddress(i)+diff);
		}
		return element;
	}
}
