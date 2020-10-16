package com.niton.collections.backed;

import com.niton.memory.direct.managed.Section;
import com.niton.memory.direct.managed.VirtualMemory;
import com.niton.memory.direct.DataStore;

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
		if(index< 0)
			throw new IndexOutOfBoundsException("Negative indexes are not allowed");
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
	public boolean contains(Object o) {
		if(size() == 0)
			return false;
		for(T e : this)
			if((e==null &&o==null) || (e != null && e.equals(o)))
			return true;
		return false;
	}

	@Override
	public int indexOf(Object o) {
		if(size() == 0)
			return -1;
		//TODO performance upgrade, serializing o and matchig the bytes agains the storage. Should yeeld good results in speed
		for (int i = 0; i < size(); i++) {
			T e = get(i);
			if((e==null && o ==null) || (e!=null&&o!=null&&e.equals(o)))
				return i;
		}
		return -1;
	}

	@Override
	public int size() {
		return (int) (memory.sectionCount()-1);
//		metaSection.jump(SIZE_POINTER);
//		try {
//			return metaReader.readInt();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
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
		memory.deleteSegment(index+1);
		writeSize(size()-1);
		return e;
	}
	@Override
	public void add(int index, T element) {
		Section sec;
		if(index == size())
			sec = memory.createOrGetSection(reservedObjectSpace,1,index+1);
		else {
			sec = memory.insertSection(index+1,reservedObjectSpace,1);
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
