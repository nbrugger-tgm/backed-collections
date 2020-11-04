package com.niton.collections.backed;

import com.niton.memory.direct.managed.BitSystem;
import com.niton.memory.direct.managed.Section;
import com.niton.memory.direct.managed.VirtualMemory;
import com.niton.memory.direct.DataStore;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class BackedList<T> extends AbstractList<T> implements RandomAccess{

	/**
	 * This size is used as a default size for an object page.
	 * Increasing the value means more memory consumption but lowers the numbers of neccessarry operations like shifting.
	 * In the best case each object has a defined size in bytes, like int=4.
	 */
	public long reservedObjectSpace = 100;
	private final VirtualMemory memory;
	private int increment = 10;

	public BackedList(DataStore store,boolean read) {
		this(store,new OOSSerializer<>(),read);
	}

	private final Serializer<T> serializer;

	public BackedList(DataStore store, Serializer<T> serializer,boolean read) {
		this.serializer = serializer;
		this.memory = new VirtualMemory(store, BitSystem.x32);
		if(read){
			memory.readIndex();
		}else{
			memory.initIndex(increment);
		}
	}

	@Override
	public boolean remove(Object o) {
		int sz = size();
		for (int i = 0; i < sz; i++) {
			Object e = get(i);
			if(o == e || Objects.equals(e,o)){
				remove(i);
				return true;
			}
		}
		return false;
	}

	@Override
	public T get(int index) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Negative indexes are not allowed");
		if(index>=size())
			throw new IndexOutOfBoundsException(index);
		Section s = memory.get(index);
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
			if(Objects.equals(o,e))
				return true;
		return false;
	}

	@Override
	public int indexOf(Object o) {
		if(size() == 0)
			return -1;

		//TODO performance upgrade, serializing o and matching the bytes against the storage.
		// Should yield good results in speed. Because its only one serialisation and then only bytematching
		// And not deserializing for every element

		for (int i = 0; i < size(); i++) {
			T e = get(i);
			if(Objects.equals(o,e))
				return i;
		}

		return -1;
	}

	@Override
	public int size() {
		return (int) (memory.sectionCount());
//		metaSection.jump(SIZE_POINTER);
//		try {
//			return metaReader.readInt();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		int sz = size();
		boolean res = false;
		for (int i = 0; i < sz; i++) {
			if(c.contains(get(i))){
				remove(i--);
				sz = size();
				res = true;
			}
		}
		return res;
	}

	@Override
	public boolean removeIf(Predicate<? super T> filter) {
		int size = size();
		boolean res = false;
		for (int i = 0; i < size; i++) {
			T e = get(i);
			if(filter.test(e)) {
				remove(i--);
				size = size();
				res = true;
			}
		}
		return res;
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
			throw new IndexOutOfBoundsException("Negative indizes are not allowed");
		if(index > size())
			throw new IndexOutOfBoundsException(index);
		Section sec;
		if(index == size())
			sec = memory.createOrGetSection(reservedObjectSpace,1,index);
		else {
			sec = memory.insertSection(index,reservedObjectSpace,1);
		}
		try {
			OutputStream os = sec.openWritingStream();
			sec.jump(0);
			serializer.write(element, os);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public T set(int index, T element) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Negative indizes are not allowed");
		if(index >= size())
			throw new IndexOutOfBoundsException(index);
		Section sec = memory.get(index);
		sec.jump(0);
		InputStream is = sec.openReadStream();
		T elem;
		OutputStream os = sec.openWritingStream();
		try {
			elem = serializer.read(is);
			sec.jump(0);
			serializer.write(element, os);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return elem;
	}

	@Override
	public void clear() {
		memory.initIndex(increment);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		int sz = size();
		boolean res = false;
		for (int i = 0; i < sz; i++) {
			T e = get(i);
			if(!c.contains(e)) {
				remove(i--);
				sz = size();
				res = true;
			}
		}
		return res;
	}

	@Override
	public void replaceAll(UnaryOperator<T> operator) {
		if(operator == null)
			throw new NullPointerException();
		long size = size();
		for (long i = 0; i < size; i++) {
			T old = get((int) i);
			set((int) i,operator.apply(old));
		}
	}
	@Override
	public Iterator<T> iterator() {
		return new BackIter(0);
	}

	@Override
	public ListIterator<T> listIterator(int i) {
		if(i>size())
			throw new IndexOutOfBoundsException(i);
		if(i<0)
			throw new IndexOutOfBoundsException("No negative indices");
		return new BackIter(i);
	}

	VirtualMemory getMemory() {
		return memory;
	}

	public void setIncrementSize(int i) {
		memory.setIndexIncrement(this.increment = i);
	}


	private class BackIter implements Iterator<T> , ListIterator<T>{
		private int elem = 0;

		public BackIter(int i) {
			elem = i;
		}

		@Override
		public boolean hasNext() {
			return elem<size() && elem>=0;
		}

		@Override
		public T next() {
			if(size()<=elem || elem < 0)
				throw new NoSuchElementException();
			removeAllowed = true;
			setAllowed = true;
			preved = false;
			return get(elem++);
		}

		@Override
		public boolean hasPrevious() {
			return elem>0&&elem<=size();
		}
		boolean preved = false;
		@Override
		public T previous() {
			preved = false;
			if(isStateLegal())
				throw new NoSuchElementException();
			removeAllowed = true;
			setAllowed = true;
			preved = true;
			return get(--elem);
		}

		@Override
		public int nextIndex() {
			return elem;
		}

		@Override
		public int previousIndex() {
			return elem-1;
		}

		@Override
		public void remove() {
			if (isStateLegal())
				throw new IllegalStateException();
			if (!removeAllowed)
				throw new IllegalStateException("remove() is only allowed afer next() or previous()");
			removeAllowed = false;
			setAllowed = false;
			BackedList.this.remove(preved?elem:elem--);
			preved = false;
		}

		private boolean isStateLegal() {
			int i = preved?0:1;
			return size() <= elem - (i) || elem - (i) < 0;
		}

		@Override
		public void set(T t) {
			if (isStateLegal())
				throw new IllegalStateException();
			if (!setAllowed)
				throw new IllegalStateException("set() is only allowed afer next() or previous()");
			BackedList.this.set(preved?elem:elem-1,t);
		}
		boolean setAllowed = false;
		boolean removeAllowed = false;
		@Override
		public void add(T t) {
			if(size()<elem || elem <0)
				throw new IllegalStateException();
			setAllowed = false;
			removeAllowed = false;
			BackedList.this.add(elem++,t);
		}
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		if(fromIndex>toIndex)
			throw new IllegalArgumentException("to cant be smaller than from");
		return new ProxyList(fromIndex,toIndex);
	}

	private class ProxyList extends AbstractList<T> {

		private int from,to;

		private ProxyList(int from, int to) {
			if(to>BackedList.this.size())
				throw new IndexOutOfBoundsException(to);
			if(from < 0)
				throw new IndexOutOfBoundsException("Negative indices forbidden");
			this.from = from;
			this.to = to;
		}

		@Override
		public T get(int index) {
			if(index < 0)
				throw new IndexOutOfBoundsException("Negative indizes are not allowed");
			if(index>=size())
				throw new IndexOutOfBoundsException(index);
			return BackedList.this.get(from+index);
		}

		@Override
		public int size() {
			return to-from;
		}

		@Override
		public T set(int index, T element) {
			if(index >= size())
				throw new IndexOutOfBoundsException(index);
			if(index < 0)
				throw new IndexOutOfBoundsException("Negative indizes are not allowed");
			return BackedList.this.set(from+index,element);
		}

		@Override
		public T remove(int index) {
			if(index >= size())
				throw new IndexOutOfBoundsException(index);
			if(index < 0)
				throw new IndexOutOfBoundsException("Negative indizes are not allowed");
			to--;
			return BackedList.this.remove(index+from);
		}

		@Override
		public void add(int index, T element) {
			if(index > size())
				throw new IndexOutOfBoundsException(index);
			if(index < 0)
				throw new IndexOutOfBoundsException("Negative indizes are not allowed");
			BackedList.this.add(from+index,element);
			to++;
		}
	}
}
