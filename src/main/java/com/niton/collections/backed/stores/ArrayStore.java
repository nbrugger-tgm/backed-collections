package com.niton.collections.backed.stores;

import java.util.Arrays;

public class ArrayStore extends FixedDataStore {
	public ArrayStore(int size) {
		this.data = new byte[size];
	}

	private byte[] data;

	@Override
	protected int maxLength() {
		return data.length;
	}

	@Override
	protected byte[] fixedInnerRead(long from, long to) {
		if(from > Integer.MAX_VALUE || to > Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("ArrayStores do not support values bigger than Integer.MAX_VALUE");
		jump(to);
		return Arrays.copyOfRange(data,(int)from,(int)to);
	}


	@Override
	protected void fixedInnerWrite(byte[] data, long from, long to) {
		if(from > Integer.MAX_VALUE || to > Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("ArrayStores do not support values bigger than Integer.MAX_VALUE");
		for (int i = (int) from; i < to; i++) {
			this.data[i] = data[(int) (i-from)];
		}
		jump(to);
	}


	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public void shiftAll(long offset) {
		shift(offset, (size()- getMarker()));
	}

}
