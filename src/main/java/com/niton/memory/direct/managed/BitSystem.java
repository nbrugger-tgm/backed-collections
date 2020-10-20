package com.niton.memory.direct.managed;

/**
 * Defines how big the addresses used are
 */
public enum BitSystem {
	x8(1),
	x16(2),
	x32(4),
	x64(8);
	private final int base;
	BitSystem(int i) {
		base = i;
	}

	public int getBase() {
		return base;
	}
}
