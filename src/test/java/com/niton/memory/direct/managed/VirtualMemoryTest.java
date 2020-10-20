package com.niton.memory.direct.managed;

import com.niton.memory.direct.stores.ArrayStore;
import org.junit.jupiter.api.*;

import java.lang.management.MemoryManagerMXBean;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class VirtualMemoryTest {
	private static ArrayStore store;
	private VirtualMemory memory;
	public static byte[] data = new byte[5];

	@BeforeAll
	static void allocateMemory(){
		//1 MB storage
		store = new ArrayStore(1024*1024);
		int seed = (int) (Math.random()*10+10);
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i+seed);
		}
	}

	@BeforeEach
	void  init() {
		store.jump(0);
		store.write(new byte[store.maxLength()]);
		store.cut(0);
		memory = new VirtualMemory(store,BitSystem.x8);
		memory.initIndex(1);
		assertEquals(memory.getBits().getBase()*4,memory.getIndex().getStartAddress(),"Index start address wrong");
		assertEquals(3*memory.getBits().getBase(),memory.getIndex().getBlockSize(),"Index block size incorrect");
	}

	@Test
	@Order(1)
	void createSection() {
		Section created = memory.createSection(1,1);
		assertDoesNotThrow(()->memory.get(0),"Error on reciving");
		assertEquals(created,memory.get(0),"Retuned wrong section");
		assertEquals(memory.getIndex().getEndAddress(),created.getStartAddress());
		assertEquals(1, created.capacity(),"Section Capacity wrong");
		assertEquals(3*memory.getBits().getBase(),memory.getIndex().size(),"Index sized improperly");
	}

	@Test
	@Order(3)
	void readIndex(){
		Section s = memory.createSection(1,1);
		Section s2 = memory.createSection(1,1);
		Section s3 = memory.createSection(1,1);
		s.shiftWrite(data);
		s2.write(data);
		s2.write(data);
		s3.write(1);
		s3.write(2);
		memory.readIndex();
		Section r1 = memory.get(0);
		Section r2 = memory.get(1);
		Section r3 = memory.get(2);
		r3.jump(0);
		r1.jump(0);
		r2.jump(0);
		s.jump(0);
		s2.jump(0);
		s3.jump(0);
		assertEquals(r1,s);
		assertEquals(r2,s2);
		assertEquals(r3,s3);
		assertEquals(2,s3.size());
		assertEquals(2,s3.capacity());
		assertEquals(1,s3.read());
		assertEquals(2,s3.read());
		assertArrayEquals(data,r1.readNext(data.length));
	}

	@Test
	@Order(2)
	void structuralTest(){
		Section s = memory.createSection(1,1);
		Section s2 = memory.createSection(1,1);
		Section s3 = memory.createSection(1,1);
		s3.write(1);
		s3.write(2);
		s.shiftWrite(data);
		s2.write(data);
		s.jump(0);
		s2.jump(0);
		s3.jump(0);
		assertEquals(2,s3.size());
		assertEquals(2,s3.capacity());
		assertEquals(1,s3.read());
		assertEquals(2,s3.read());
		assertArrayEquals(data,s.readNext(data.length));
		assertArrayEquals(data,s2.readNext(data.length));
	}

	@Test
	@Order(5)
	void deleteSegment() {
		memory.createSection(2,2);
		memory.createSection(2,2);
		memory.get(0).write(1);
		memory.get(0).write(2);
		memory.get(1).write(2);
		memory.get(1).write(1);
		memory.deleteSegment(0);
		assertEquals(1,memory.sectionCount());
		assertEquals(memory.getIndex().getEndAddress(),memory.get(0).getStartAddress());
		assertEquals(4, memory.get(0).capacity());
		memory.get(0).jump(0);
		assertEquals(2,memory.get(0).read());
		assertEquals(1,memory.get(0).read());


		memory.initIndex(2);
		memory.createSection(1,1);
		memory.createSection(1,1);
		memory.createSection(1,1);
		memory.get(0).write(1);
		memory.get(1).write(2);
		memory.get(2).write(3);
		memory.deleteSegment(0);
		assertEquals(2, memory.get(0).read());
		assertEquals(3, memory.get(1).read());
		memory.deleteSegment(1);
		assertEquals(3, memory.get(0).read());
	}

	@Test
	@Order(4)
	void insertSection() {
		Section orig1 = memory.createSection(1,1);
		Section orig2 = memory.createSection(1,1);
		orig1.write(data);
		orig2.write(data);
		long oldIndexSize = memory.getIndex().size();
		Section neww = memory.insertSection(1,5,1);
		assertEquals(oldIndexSize+memory.getSectionHeaderSize(),memory.getIndex().size());
		assertEquals(3,memory.sectionCount());

		assertEquals(data.length, memory.get(0).size());
		assertEquals(data.length, memory.get(2).size());

		Section inserted = memory.get(1);
		assertEquals(memory.get(0).getEndAddress(), inserted.getStartAddress());
		assertEquals(memory.get(2).getStartAddress(), inserted.getEndAddress());

		assertArrayEquals(data,memory.get(0).readNext(data.length));
		assertArrayEquals(data,memory.get(2).readNext(data.length));

		assertEquals(5,inserted.getBlockSize());
		assertEquals(0,inserted.size());

		inserted.write(data);
		inserted.write(data);
		assertEquals(data.length*2, inserted.size());
		assertDoesNotThrow(()->memory.readIndex());
	}

}