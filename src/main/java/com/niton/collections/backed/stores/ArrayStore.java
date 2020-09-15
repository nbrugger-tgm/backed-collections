package com.niton.collections.backed.stores;

import com.niton.collections.backed.DataStore;

import java.util.Arrays;

public class ArrayStore extends DataStore {
	private int end = 0;
	public ArrayStore(int size) {
		this.data = new byte[size];
	}

	private byte[] data;
	@Override
	public int size() {
		return Math.min(end,data.length);
	}

	@Override
	protected byte[] innerRead(int from, int to) {
		jump(to);
		return Arrays.copyOfRange(data,from,to);
	}

	@Override
	protected void innerWrite(byte[] data, int from, int to) {
		for (int i = from; i < to; i++) {
			this.data[i] = data[i-from];
		}
		jump(to);
		end = Math.max(to,end);
	}

	@Override
	public int cut(int from) {
		jump(from);
		int counter = 0;
		while (getMarker() < size()) {
			counter++;
			write(0);
		}
		end = from;
		return counter;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public void shiftAll(int offset) {
		shift(offset, (size()- getMarker())-offset);
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("ArrayStore->");
		if (data == null) sb.append("null");
		else {
			sb.append('[');
			for (int i = 0; i < data.length; ++i)
				sb.append(i == 0 ? "" : ", ").append(i == getMarker()?"> ":"").append(data[i]);
			sb.append(']');
		}
		return sb.toString();
	}
}
