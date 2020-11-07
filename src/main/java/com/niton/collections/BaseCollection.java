package com.niton.collections;

import com.niton.collections.backed.BackedMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class BaseCollection<E> implements Collection<E> {
	@Override
	public boolean containsAll(Collection<?> c) {
		return c.stream().map(this::contains).reduce(true,(a,b)->a && b);
	}@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("[");
		sb.append(this.stream().map(Objects::toString).collect(Collectors.joining(", ")));
		sb.append(']');
		return sb.toString();
	}@Override
	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	@Override
	public <T> T[] toArray(T[] a) {
		int sz = size();
		if(size()<=a.length) {
			Arrays.fill(a,null);
		}else{
			a = Arrays.copyOf(a,sz);
		}
		for (int i = 0; i < sz; i++) {
			a[i] = (T)get(i);
		}
		return a;
	}
	@Override
	public boolean removeAll(Collection<?> c) {
		int prevSize = size();
		c.forEach(e-> {
			while (contains(e)) this.remove(e);
		});
		return size()-prevSize != 0;
	}

	protected abstract E get(int i);
}
