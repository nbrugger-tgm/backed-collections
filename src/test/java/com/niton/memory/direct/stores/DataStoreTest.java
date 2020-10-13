package com.niton.memory.direct.stores;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
public abstract class DataStoreTest {
	protected DataStore store = createDataStoreImpl();

	protected abstract DataStore createDataStoreImpl();

	@Test
	void cut() {
		store.jump(0);
		store.write(new byte[]{1,2,3,4,5,6,7,8,9,1,2,3,4});
		store.cut(5);
		assertEquals(5, store.size());
		assertEquals(store.size(), store.getMarker());
		store.jump(9);
		store.write(0);
		assertEquals(10,store.size());
		store.jump(6);
		store.write(0);
		assertEquals(10,store.size());
		store.jump(0);
		store.cut();
		assertEquals(0, store.size());
		assertEquals(store.size(), store.getMarker());
	}

	@Test
	void shiftAll() {
		store.jump(0);
		Stream.of(1,2,3,4,5,6,7,8,9,10).map(i ->i-Byte.MIN_VALUE).forEach(store::write);
		store.cut();
		store.jump(0);
		long oldSize = store.size();
		store.shiftAll(3);
		assertEquals(3, store.getMarker(),"Byte marker displaced");
		assertEquals(oldSize+3, store.size());
		assertTrue(Arrays.equals(new byte[]{1,2,3,4,5,6,7,8,9,10},store.readNext(10)));
	}
	@Test
	void shiftWrite(){
		store.jump(0);
		store.cut();
		store.write(new byte[]{6,7,8,9,10});
		store.jump(0);
		store.shiftWrite(new byte[]{1,2,3,4,5});
		assertEquals(5, store.getMarker());
		assertEquals(10, store.size(),"Incorrect size after shift write");
		store.jump(0);
		assertArrayEquals(new byte[]{1,2,3,4,5},store.readNext(5),"Values incorrect written or read");
		store.jump(0);
		assertArrayEquals(new byte[]{1,2,3,4,5,6,7,8,9,10},store.readNext(10),"Values incorrect written or read");
		store.jump(0);
		Stream.of(1,2,3,4,5).forEach(e->store.shiftWrite(e-Byte.MIN_VALUE));
		store.jump(0);
		assertTrue(Arrays.equals(new byte[]{1,2,3,4,5},store.readNext(5)));
	}
	@Test
	void fullTest(){
		store.jump(0);
		assertEquals(0,store.getMarker(),"Jump not correctly executed");
		store.write(new byte[]{1,2,3,4,5,6,7,8,9,10});
		assertEquals(10,store.size(),"Wrong size after writing");
		store.jump(0);
		byte[] content = store.readNext(10);
		assertArrayEquals(new byte[]{1,2,3,4,5,6,7,8,9,10},content,"Read returned bad wrong data");
		store.write(200);
		assertEquals(11,store.size(),"Store not properly enlarged on write");
		store.jump(10);
		assertEquals(200,store.read(),"Read retuned unexpected value");
		store.jump(0);
		long presize = store.size();
		store.write(new byte[]{1,2,3,4,5});
		store.jump(0);
		store.shiftAll(10);
		assertEquals(presize+10, store.size(),"Store not properly enlarged after shift");
		store.jump(10);
		assertArrayEquals(new byte[]{1,2,3,4,5},store.readNext(5),"Wrong reads after shift");
	}

	@Test
	void streaming() throws IOException {
		store.jump(0);
		store.cut();
		OutputStream writer = store.openWritingStream();
		OutputStream shiftWriter = store.openShiftWritingStream();
		InputStream reader = store.openReadStream();
		for (int i = 0; i < 4; i++) {
			writer.write(new byte[]{(byte)i});
		}
		assertEquals(4, store.size());
		assertEquals(4,store.getMarker());
		store.jump(0);
		assertArrayEquals(new byte[]{0,1,2,3}, store.readNext(4));
		store.jump(0);
		for (int i = 0; i <4; i++) {
			byte[] read = new byte[1];
			reader.read(read);
			assertEquals((byte)i, read[0]);
		}
		store.jump(1);
		for (int i = 10; i < 50; i+=10) {
			shiftWriter.write(new byte[]{(byte)i});
		}
		store.jump(0);
		assertArrayEquals(new byte[]{0,10,20,30,40,1,2,3}, store.readNext(8));

		store.jump(0);
		byte min = store.stream().min(Byte::compare).get();
		store.jump(0);
		byte max = store.stream().max(Byte::compare).get();
		assertEquals(store.stream().distinct().count(),8L);
		assertEquals(40,max);
		assertEquals(0, min);
	}
}