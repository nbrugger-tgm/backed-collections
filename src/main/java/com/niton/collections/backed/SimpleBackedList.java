package com.niton.collections.backed;

import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.managed.BitSystem;
import com.niton.memory.direct.managed.Section;
import com.niton.memory.direct.managed.VirtualMemory;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class SimpleBackedList<T> extends AbstractList<T> implements RandomAccess{

	/**
	 * This size is used as a default size for an object page.
	 * Increasing the value means more memory consumption but lowers the numbers of neccessarry operations like shifting.
	 * In the best case each object has a defined size in bytes, like int=4.
	 */
	public long reservedObjectSpace = 100;
	private final VirtualMemory memory;
	public SimpleBackedList(DataStore store, boolean read) {
		this(store,new OOSSerializer<>(),read);
	}

	private final Serializer<T> serializer;

	public SimpleBackedList(DataStore store, Serializer<T> serializer, boolean read) {
		this.serializer = serializer;
		this.memory = new VirtualMemory(store, BitSystem.x32);
		if(read){
			memory.readIndex();
		}else{
			memory.initIndex(10);
		}
	}

	@Override
	public int size() {
		return (int) (memory.sectionCount());
	}
	@Override
	public T remove(int index) {
		T e = get(index);
		memory.deleteSegment(index);
		return e;
	}
	@Override
	public void add(int index, T element) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Negative indices are not allowed");
		Section sec;
		if(index == size())
			sec = memory.createOrGetSection(reservedObjectSpace,1,index);
		else {
			sec = memory.insertSection(index,reservedObjectSpace,1);
		}

		sec.write(element,serializer);
	}

	@Override
	public T get(int index) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Negative indices are not allowed");
		Section sec = memory.get(index);
		return sec.read(serializer);
	}

	@Override
	public T set(int index, T element) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Negative indices are not allowed");
		Section sec = memory.get(index);

		T elem = sec.read(serializer);
		sec.write(element,serializer);
		return elem;
	}

	@Override
	public void clear() {
		memory.initIndex(10);
	}
}
