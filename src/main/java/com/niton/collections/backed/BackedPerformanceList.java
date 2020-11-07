package com.niton.collections.backed;

import com.niton.StorageException;
import com.niton.collections.BaseCollection;
import com.niton.collections.ProxyList;
import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.NegativeIndexException;
import com.niton.memory.direct.managed.BitSystem;
import com.niton.memory.direct.managed.Section;
import com.niton.memory.direct.managed.VirtualMemory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class BackedPerformanceList<T> extends BaseCollection<T> implements List<T> {
	private final VirtualMemory mainMemory;
	private final BackedMap<Integer,Integer> indexMap;
	private final VirtualMemory dataMemory;
	private final Serializer<T> serializer;
	public BackedPerformanceList(DataStore store, boolean read, Serializer<T> serializer) {
		mainMemory = new VirtualMemory(store, BitSystem.X64);
		this.serializer = serializer;
		if(read) {
			mainMemory.readIndex();
		}else{
			store.cut(0);
			mainMemory.initIndex(2);
		}
		Section indexSection = read ? mainMemory.get(0) : mainMemory.createSection(1024*1024*5,4);
		indexMap = new BackedMap<>(indexSection,Serializer.INT,Serializer.INT,read);
		indexMap.useCaching = true;
		indexMap.setKEY_SIZE_ALLOC(4);
		indexMap.setVALUE_SIZE_ALLOC(4);
		Section dataSection = read ? mainMemory.get(1) : mainMemory.createSection(1024*1024*5,1);
		dataMemory = new VirtualMemory(dataSection, BitSystem.X64);
		if(read)
			dataMemory.readIndex();
		else
			dataMemory.initIndex(1024);
	}


	@Override
	public int size() {
		return indexMap.size();
	}

	@Override
	public boolean isEmpty() {
		return indexMap.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o)!=-1;
	}

	@Override
	public Iterator<T> iterator() {
		return listIterator();
	}


	@Override
	public boolean add(T t) {
		int index = size();
		int dataIndex = getNextFreeSegmentIndex();
		Section sect = dataMemory.createOrGetSection(2*1024,4,dataIndex);
		sect.write(t,serializer);
		this.indexMap.put(index,dataIndex);
		return true;
	}

	private int getNextFreeSegmentIndex() {
		int index = 0;
		Collection<Integer> usedIndexes = this.indexMap.values();
		while (usedIndexes.contains(index))
			index++;
		return index;
	}

	@Override
	public boolean remove(Object o) {
		int sz = size();
		try{
			int i = indexOf((T)o);
			if(i == -1)
				return false;
			remove(indexOf(o));
		}catch (ClassCastException e){
			return false;
		}
		return sz-size()>0;
	}



	@Override
	public boolean addAll(Collection<? extends T> c) {
		for (T t : c) {
			add(t);
		}
		return c.size()>0;
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		int i = 0;
		for (T t : c) {
			add(index+(i++),t);
		}
		return c.size()>0;
	}


	@Override
	public boolean retainAll(Collection<?> c) {
		boolean acc = false;
		for (int i = 0; i <size(); i++) {
			Object o = get(i);
			if(!c.contains(o)) {
				remove(i--);
				acc = true;
			}
		}
		return acc;
	}

	@Override
	public void clear() {
		indexMap.clear();
	}

	@Override
	public T get(int i) {
		if(i >= size())
			throw new IndexOutOfBoundsException(i);
		if(i < 0)
			throw new IndexOutOfBoundsException(i);
		return dataMemory.get(this.indexMap.get(i)).read(serializer);
	}

	@Override
	public T set(int index, T element) {
		if(index >= size())
			throw new IndexOutOfBoundsException(index);
		if(index < 0)
			throw new IndexOutOfBoundsException(index);
		index = this.indexMap.get(index);
		Section sect = dataMemory.get(index);
		T old  = sect.read(serializer);
		sect.write(element,serializer);
		return old;
	}

	@Override
	public void add(int index, T element) {
		if(index < 0)
			throw new NegativeIndexException();
		if(index == size())
			add(element);
		else if (index > size())
			throw new IndexOutOfBoundsException("Cannot add element beyond end");
		else {
			shiftUp(index,1);
			int sectIndex = getNextFreeSegmentIndex();
			indexMap.put(index, sectIndex);
			dataMemory.createOrGetSection(2*1024,4,sectIndex).write(element,serializer);
		}
	}


	@Override
	public T remove(int index) {
		if(index < 0)
			throw new NegativeIndexException();
		if(index >= size())
			throw new IndexOutOfBoundsException(index);

		T t = get(index);

		dataMemory.get(indexMap.get(index)).cut(0);
		shiftDown(index,1);
		indexMap.remove(size()-1);

		return t;
	}
	private void shiftUp(int index, int i) {
		if(index < 0)
			throw new NegativeIndexException();
		int targetIndex = size()+i-1;
		while(targetIndex > index) {
			int fetchFrom = targetIndex-i;
			int indexFrom = indexMap.get(fetchFrom);
			this.indexMap.put(targetIndex, indexFrom);
			targetIndex--;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof List)) return false;
		if(((List<?>) o).size() != size()) return false;
		int i = 0;
		while(i<size()){
			if(!Objects.equals(((List<?>) o).get(i),get(i++)))
			return false;
		}
			return true;
	}

	@Override
	public int hashCode() {
		int i = 0;
		int expectedHashCode = 1;
		while (i<size()) {
			T element = get(i++);
			expectedHashCode = 31 * expectedHashCode + ((element == null) ? 0 : element.hashCode());
		}
		return expectedHashCode;
	}

	private void shiftDown(int index, int i) {
		if(index < 0)
			throw new NegativeIndexException();
		int targetIndex = index;
		int sz = size();
		while(targetIndex < sz-1) {
			this.indexMap.replace(targetIndex, indexMap.get(targetIndex + i));
			targetIndex++;
		}
	}

	@Override
	public int indexOf(Object o) {
		byte[] serial;
		try {
			serial = serialize((T) o);
		}catch (ClassCastException c){
			return -1;
		} catch (IOException e) {
			throw new StorageException(e);
		}

		for (int i = 0; i < dataMemory.sectionCount(); i++) {
			Section s = dataMemory.get(i);
			byte[] content = s.readFull();
			if(Arrays.equals(content,serial))
				return getIndexForSection(i);
		}
		return -1;
	}

	private byte[] serialize(T o) throws IOException {
		byte[] serial;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		serializer.write(o, bos);
		serial = bos.toByteArray();
		return serial;
	}

	private int getIndexForSection(int i) {
		for (Integer index : indexMap.keySet()) {
			if(indexMap.get(index) == i)
				return index;
		}
		return -1;
	}

	@Override
	public int lastIndexOf(Object o) {
		byte[] serial;
		try {
			serial = serialize((T) o);
		}catch (ClassCastException c){
			return -1;
		} catch (IOException e) {
			throw new StorageException(e);
		}
		int res = -1;
		for (int i = 0; i < dataMemory.sectionCount(); i++) {
			Section s = dataMemory.get(i);
			byte[] content = s.readFull();
			if(Arrays.equals(content,serial)) {
				int present = getIndexForSection(i);
				if(present>res)
					res = present;
			}
		}
		return res;
	}

	@Override
	public ListIterator<T> listIterator() {
		return listIterator(0);
	}

	private class Itr implements Iterator<T> {
		/**
		 * Index of element to be returned by subsequent call to next.
		 */
		int cursor = 0;

		/**
		 * Index of element returned by most recent call to next or
		 * previous.  Reset to -1 if this element is deleted by a call
		 * to remove.
		 */
		int lastRet = -1;

		/**
		 * The modCount value that the iterator believes that the backing
		 * List should have.  If this expectation is violated, the iterator
		 * has detected concurrent modification.
		 */
		int expectedModCount = size();

		public boolean hasNext() {
			return cursor != size();
		}

		public T next() {
			checkForComodification();
			try {
				int i = cursor;
				T next = get(i);
				lastRet = i;
				cursor = i + 1;
				return next;
			} catch (IndexOutOfBoundsException e) {
				checkForComodification();
				throw new NoSuchElementException();
			}
		}

		public void remove() {
			if (lastRet < 0)
				throw new IllegalStateException();
			checkForComodification();

			try {
				BackedPerformanceList.this.remove(lastRet);
				if (lastRet < cursor)
					cursor--;
				lastRet = -1;
				expectedModCount = size();
			} catch (IndexOutOfBoundsException e) {
				throw new ConcurrentModificationException();
			}
		}

		final void checkForComodification() {
			if (size() != expectedModCount)
				throw new ConcurrentModificationException();
		}
	}

	private class ListItr extends Itr implements ListIterator<T> {
		ListItr(int index) {
			cursor = index;
		}

		public boolean hasPrevious() {
			return cursor != 0;
		}

		public T previous() {
			checkForComodification();
			try {
				int i = cursor - 1;
				T previous = get(i);
				lastRet = cursor = i;
				return previous;
			} catch (IndexOutOfBoundsException e) {
				checkForComodification();
				throw new NoSuchElementException();
			}
		}

		public int nextIndex() {
			return cursor;
		}

		public int previousIndex() {
			return cursor-1;
		}

		public void set(T e) {
			if (lastRet < 0)
				throw new IllegalStateException();
			checkForComodification();

			try {
				BackedPerformanceList.this.set(lastRet, e);
				expectedModCount = size();
			} catch (IndexOutOfBoundsException ex) {
				throw new ConcurrentModificationException();
			}
		}

		public void add(T e) {
			checkForComodification();

			try {
				int i = cursor;
				BackedPerformanceList.this.add(i, e);
				lastRet = -1;
				cursor = i + 1;
				expectedModCount = size();
			} catch (IndexOutOfBoundsException ex) {
				throw new ConcurrentModificationException();
			}
		}
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		if(index > size())
			throw new IndexOutOfBoundsException(index);
		if(index < 0)
			throw new NegativeIndexException();
		return new ListItr(index);
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		if(fromIndex>toIndex)
			throw new IllegalArgumentException("to cant be smaller than from");
		return new ProxyList(this, fromIndex,toIndex);
	}
}
