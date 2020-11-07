package com.niton.collections;

import java.util.*;

public class DefaultIterator<T> implements Iterator<T>, ListIterator<T> {
	/**
	 * Index of element to be returned by subsequent call to next.
	 */
	int cursor = 0;
	private final List<T> base;

	/**
	 * Index of element returned by most recent call to next or
	 * previous.  Reset to -1 if this element is deleted by a call
	 * to remove.
	 */
	int lastRet = -1;
	int expectedModCount = 0;

	public DefaultIterator(List<T> base,int index) {
		this.base = base;
		cursor = index;
		expectedModCount = base.size();
	}

	public boolean hasNext() {
		return cursor != base.size();
	}

	public T next() {
		checkForComodification();
		try {
			int i = cursor;
			T next = base.get(i);
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
			base.remove(lastRet);
			if (lastRet < cursor)
				cursor--;
			lastRet = -1;
			expectedModCount = base.size();
		} catch (IndexOutOfBoundsException e) {
			throw new ConcurrentModificationException();
		}
	}

	final void checkForComodification() {
		if (base.size() != expectedModCount)
			throw new ConcurrentModificationException();
	}

	public boolean hasPrevious() {
		return cursor != 0;
	}

	public T previous() {
		checkForComodification();
		try {
			int i = cursor - 1;
			T previous = base.get(i);
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
			base.set(lastRet, e);
			expectedModCount = base.size();
		} catch (IndexOutOfBoundsException ex) {
			throw new ConcurrentModificationException();
		}
	}

	public void add(T e) {
		checkForComodification();

		try {
			int i = cursor;
			base.add(i, e);
			lastRet = -1;
			cursor = i + 1;
			expectedModCount = base.size();
		} catch (IndexOutOfBoundsException ex) {
			throw new ConcurrentModificationException();
		}
	}
}
