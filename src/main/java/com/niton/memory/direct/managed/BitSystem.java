package com.niton.memory.direct.managed;

import com.niton.memory.direct.DataStore;

import java.io.DataOutputStream;
import java.io.IOException;

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


	public void write(long address, long i, DataStore store,DataOutputStream dos) {
		store.jump(address);
		try {
			switch (this){
				case x8:
					dos.writeByte((int) i);
					break;
				case x16:
					dos.writeShort((int) i);
					break;
				case x32:
					dos.writeInt((int) i);
					break;
				case x64:
					dos.writeLong(i);
					break;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	public void write(long address, long i, DataStore store) {
		DataOutputStream dos = new DataOutputStream(store.openWritingStream());
		write(address,i,store,dos);
	}
}
