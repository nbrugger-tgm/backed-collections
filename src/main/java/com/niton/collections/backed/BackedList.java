package com.niton.collections.backed;

import com.niton.collections.backed.managed.Section;
import com.niton.collections.backed.managed.VirtualMemory;
import com.niton.collections.backed.stores.DataStore;
import com.niton.collections.backed.streams.CounterOutputStream;

import java.io.*;
import java.util.AbstractList;
import java.util.RandomAccess;

public class BackedList<T> extends AbstractList<T> implements RandomAccess{

	/**
	 * This size is used as a default size for an object page.
	 * Increasing the value means more memory consumption but lowers the numbers of neccessarry operations like shifting.
	 * In the best case each object has a defined size in bytes, like int=4.
	 */
	public long reservedObjectSpace = 100;
	private final VirtualMemory memory;
	private final Section metaSection;
	private final DataOutputStream metaWriter;
	private final DataInputStream metaReader;
	private static final int SIZE_POINTER = 0;
	public BackedList(DataStore store,boolean read) {
		this(store,new OOSSerializer<>(),read);
	}

	private final Serializer<T> serializer;

	public BackedList(DataStore store, Serializer<T> serializer,boolean read) {
		this.serializer = serializer;
		this.memory = new VirtualMemory(store);
		if(read){
			memory.readIndex();
			metaSection = memory.get(0);
		}else{
			memory.initIndex(10);
			metaSection = memory.createSection(256,1);
		}
		metaReader = new DataInputStream(metaSection.openReadStream());
		metaWriter = new DataOutputStream(metaSection.openWritingStream());
		if (!read){
			writeSize(0);
		}
	}

	@Override
	public T get(int index) {
		if(index>size())
			return  null;
		Section s = memory.get(index+1);
		s.jump(0);
		try {
			return serializer.read(s.openReadStream());
		} catch (Exception e) {
			throw new RuntimeException("Backing Exception",e);
		}
	}

	@Override
	public int size() {
		metaSection.jump(SIZE_POINTER);
		try {
			return metaReader.readInt();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	private void writeSize(int sz){
		metaSection.jump(SIZE_POINTER);
		try {
			metaWriter.writeInt(sz);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public T remove(int index) {
		T e = get(index);
		int sz = size();
		if(index+1<sz)
			shiftElements(index+1,-1);
		writeSize(sz-1);
		return e;
	}

	private void shiftElements(int i, int shift) {
		if(shift == 0)
			return;
		if(shift<0){
			int sz = size();
			for (int j = i; j < sz; j++) {
				T e = get(j);
				set(j+shift,e);
			}
		}else{
			for (int j = i; j >= 0; j--) {
				T e = get(j);
				if(j+shift>size())
					add(j+shift, e);
				else
					set(j+shift,e);
			}
		}
	}

	@Override
	public void add(int index, T element) {
		Section sec;
		if(index == size())
			if(memory.readSize()-1 <= index)
				sec = memory.createSection(reservedObjectSpace,1);
			else
				sec = memory.get(index+1);
		else {
			shiftElements(index, 1);
			sec = memory.get(index+1);
		}
		try {
			OutputStream os = sec.openWritingStream();
			sec.jump(0);
			serializer.write(element, os);
			int sz = size();
			writeSize(sz+1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public T set(int index, T element) {
		Section sec = memory.get(index+1);
		sec.jump(0);
		OutputStream os = sec.openWritingStream();
		try {
			serializer.write(element, os);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return element;
	}
}
