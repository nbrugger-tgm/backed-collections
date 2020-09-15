package com.niton.collections.backed.streams;

import java.io.IOException;
import java.io.OutputStream;

public class CounterOutputStream extends OutputStream {
	private int counter = 0;
	private OutputStream out;

	public CounterOutputStream(OutputStream out) {
		this.out = out;
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
		counter++;
	}

	public int getCounter() {
		return counter;
	}

	public void resetCounter() {
		counter = 0;
	}
}
