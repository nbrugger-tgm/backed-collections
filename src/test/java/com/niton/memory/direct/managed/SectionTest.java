package com.niton.memory.direct.managed;

import com.niton.memory.direct.stores.ArrayStore;
import com.niton.memory.direct.stores.DataStore;
import com.niton.memory.direct.stores.DataStoreTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
public class SectionTest extends DataStoreTest {

	private ArrayStore baseStorage;

	@Test
	@Order(1)
	void init() {
		Section sect = (Section) store;
		assertTrue(baseStorage.size()>=16);
		assertEquals(1024,sect.getBlockSize());
		assertEquals(Section.HEADER_SIZE, sect.getStartAddress());
		assertEquals(0, (int)sect.size());
		assertEquals(sect.getStartAddress()+1024, sect.getEndAddress());
	}
	@BeforeEach
	void prepare(){
		Section sect = (Section) store;
		sect.init(1024,1,Section.HEADER_SIZE);
		System.out.println(sect);
		System.out.println(baseStorage);
	}

	@Test
	void addBlock() {
		Section sect = (Section) store;
		sect.addBlock();
		assertEquals(2048, sect.capacity());
	}

	@Test
	void autoAddBlock(){
		Section sect = (Section) store;
		for(int i = 0;i<1500;i++)
			store.write(i%200);
		assertEquals(2048, sect.capacity());
		System.out.println(store);
		store.jump(0);
		for(int i = 0;i<1500;i++)
			assertEquals(i%200,store.read());
	}

	@Override
	protected DataStore createDataStoreImpl() {
		baseStorage = new ArrayStore(1024*8);
		return new Section(0, baseStorage);
	}
}