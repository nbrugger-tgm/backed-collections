package com.niton.collections;

import java.util.*;

public abstract class BaseList<T> extends BaseCollection<T> implements List<T> {
	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		for (T t : c) {
			add(index,t);
		}
		return c.size()>0;
	}


	@Override
	public int indexOf(Object o) {
		for (int i = 0; i < size(); i++) {
			if(get(i).equals(o))
				return i;
		}
		return -1;
	}


	@Override
	public int lastIndexOf(Object o) {
		for (int i = size()-1; i >= 0; i--) {
			if(get(i).equals(o))
				return i;
		}
		return -1;
	}

	@Override
	public ListIterator<T> listIterator() {
		return new DefaultIterator<>(this,0);
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		return new DefaultIterator<>(this,index);
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return new ProxyList<>(this,fromIndex,toIndex);
	}

	@Override
	public boolean isEmpty() {
		return size()==0;
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o)!=-1;
	}

	@Override
	public Iterator<T> iterator() {
		return new DefaultIterator<>(this,0);
	}

	@Override
	public boolean add(T t) {
		int size;
		add(size = size(),t);
		return size()>size;
	}

	@Override
	public boolean remove(Object o) {
		int size = size();
		remove(indexOf(o));
		return size()-size != 0;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		if(c.size() == 0)
			return false;
		return c.stream().map(this::add).reduce((a,b)->a|b).get();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean ch = false;
		int i = 0;
		while(i<size()){
			T e = get(0);
			if(!c.contains(e)){
				remove(i--);
				ch = true;
			}
			i++;
		}
		return ch;
	}
}
