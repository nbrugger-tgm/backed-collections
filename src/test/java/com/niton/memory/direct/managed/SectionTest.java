package com.niton.memory.direct.managed;

import com.niton.memory.direct.stores.ArrayStore;
import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.stores.DataStoreTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
public class SectionTest extends DataStoreTest {

	private ArrayStore baseStorage;
	public static byte[] data = new byte[10];
	@Test
	@Order(1)
	void init() {
		Section sect = (Section) store;
		assertTrue(baseStorage.size()>=sect.getBit().getBase()*4);
		assertEquals(20,sect.getBlockSize());
		assertEquals(sect.getHeaderSize(), sect.getStartAddress());
		assertEquals(0, (int)sect.size());
		assertEquals(sect.getStartAddress()+20, sect.getEndAddress());
	}
	@BeforeEach
	void prepare(){
		Section sect = (Section) store;
		sect.init(20,1,sect.getHeaderSize());
		System.out.println(sect);
		System.out.println(baseStorage);
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)i;
		}
	}

	@Test
	void addBlock() {
		Section sect = (Section) store;
		sect.addBlock();
		assertEquals(40, sect.capacity());
	}

	@Test
	void autoAddBlock(){
		Section sect = (Section) store;
		for(int i = 0;i<30;i++)
			store.write(i%200);
		assertEquals(40, sect.capacity());
		System.out.println(store);
		store.jump(0);
		for(int i = 0;i<30;i++)
			assertEquals(i%200,store.read());
	}

	@Override
	protected DataStore createDataStoreImpl() {
		baseStorage = new ArrayStore(1024*8);
		return new Section(0, baseStorage,BitSystem.X8);
	}

	@Test
	void shift() {
		Section sect = (Section) store;
		sect.write(data);
		long start = sect.getStartAddress();
		long size = sect.size();
		long capa = sect.capacity();
		sect.shiftForward(30);
		assertEquals(start+30, sect.getStartAddress());
		assertEquals(size,sect.size());
		assertEquals(capa,sect.capacity());
		assertArrayEquals(data,sect.read(0,data.length));
		sect.shiftBack(20);assertEquals(size,sect.size());
		assertEquals(capa,sect.capacity());
		assertArrayEquals(data,sect.read(0,data.length));
	}
}