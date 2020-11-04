package com.niton.memory.direct.managed;

import com.niton.StorageException;
import com.niton.memory.direct.DataStore;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Defines how big the addresses used are
 */
public enum BitSystem {
	X8(1),
	X16(2),
	X32(4),
	X64(8);
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
				case X8:
					dos.writeByte((int) i);
					break;
				case X16:
					dos.writeShort((int) i);
					break;
				case X32:
					dos.writeInt((int) i);
					break;
				case X64:
					dos.writeLong(i);
					break;
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	public void write(long address, long i, DataStore store) {
		DataOutputStream dos = new DataOutputStream(store.openWritingStream());
		write(address,i,store,dos);
	}
}
