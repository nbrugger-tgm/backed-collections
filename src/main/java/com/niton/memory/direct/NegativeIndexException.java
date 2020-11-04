package com.niton.memory.direct;

public class NegativeIndexException extends IndexOutOfBoundsException {
	public NegativeIndexException() {
		super("Negative indices are not allowed");
	}
}
