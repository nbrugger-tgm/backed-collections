package com.niton.collections.backed.streams;

import java.io.IOException;
import java.io.InputStream;

public class CounterInputStream extends InputStream {
	private InputStream sub;
	private int count = 0;

	public CounterInputStream(InputStream sub) {
		this.sub = sub;
	}

	@Override
	public int read() throws IOException {
		int b = sub.read();
		count++;
		return b;
	}

	public int getCount() {
		return count;
	}
}
