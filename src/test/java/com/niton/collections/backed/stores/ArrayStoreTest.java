package com.niton.collections.backed.stores;

import com.niton.collections.backed.DataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class ArrayStoreTest {
	private ArrayStore store;
	private ArrayStore bigStore;
	@BeforeEach
	public void init(){
		store = new ArrayStore(5);
		store.write(new byte[]{1,2,3,4,5});
	}

	@Test
	void innerRead() {
	}

	@Test
	void innerWrite() {
	}

	@Test
	void cut() {
	}

	@Test
	void shiftAll() {
		store.jump(1);
		store.shiftAll(1);
	}
	@Test
	void bigShiftAll() {
		int oldBuf = store.bufferSize;
		store.bufferSize = 2;
		System.out.println("Original : "+store);
		assertDoesNotThrow(()->{
			store.jump(1);
			store.shiftAll(1);
			System.out.println("Shifted "+store);
		});
		assertDoesNotThrow(()->{
			store.jump(1);
			store.shiftAll(-1);
			System.out.println("Shifted "+store);
		});
		store.bufferSize = oldBuf;
	}
	@Test
	void shiftWrite(){
		store.jump(1);
		store.shiftWrite(200);
	}
}