package com.niton.collections;

import com.niton.collections.backed.BackedList;

import java.util.AbstractList;
import java.util.List;

public class ProxyList<T> extends AbstractList<T> {

	private final List<T> refList;
	private int from, to;

	public ProxyList(List<T> backedList, int from, int to) {
		this.refList = backedList;
		if (to > backedList.size())
			throw new IndexOutOfBoundsException(to);
		if (from < 0)
			throw new IndexOutOfBoundsException("Negative indices forbidden");
		this.from = from;
		this.to = to;
	}

	@Override
	public T get(int index) {
		if (index < 0)
			throw new IndexOutOfBoundsException("Negative indizes are not allowed");
		if (index >= size())
			throw new IndexOutOfBoundsException(index);
		return refList.get(from + index);
	}

	@Override
	public int size() {
		return to - from;
	}

	@Override
	public T set(int index, T element) {
		if (index >= size())
			throw new IndexOutOfBoundsException(index);
		if (index < 0)
			throw new IndexOutOfBoundsException("Negative indizes are not allowed");
		return refList.set(from + index, element);
	}

	@Override
	public T remove(int index) {
		if (index >= size())
			throw new IndexOutOfBoundsException(index);
		if (index < 0)
			throw new IndexOutOfBoundsException("Negative indizes are not allowed");
		to--;
		return refList.remove(index + from);
	}

	@Override
	public void add(int index, T element) {
		if (index > size())
			throw new IndexOutOfBoundsException(index);
		if (index < 0)
			throw new IndexOutOfBoundsException("Negative indizes are not allowed");
		refList.add(from + index, element);
		to++;
	}
}
