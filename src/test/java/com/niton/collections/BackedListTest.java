package com.niton.collections;

import com.niton.collections.backed.BackedList;
import com.niton.collections.backed.Serializer;
import com.niton.memory.direct.stores.ArrayStore;
import com.niton.memory.direct.stores.FixedDataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BackedListTest {

	BackedList<String> list;

	@BeforeEach
	public void init() {
		list = new BackedList<>(new ArrayStore(1024*1024),Serializer.STRING,false);
	}
	@Test
	public void general(){
		list.add("Wir");
		list.add("Strings");
		list.add("sind");
		assertEquals(3,list.size());
		assertEquals("Strings",list.get(1));
		list.remove(1);
		assertEquals(2,list.size());
		list.clear();
		assertEquals(0, list.size());
		assertThrows(Throwable.class, ()->list.get(0));
		assertThrows(Throwable.class,()->list.remove(0));
	}

	@Test
	public void testOverflow(){
		assertThrows(FixedDataStore.MemoryOverflowException.class,()->{
			for (int i = 0; i < 10000; i++) {
				list.add("Some weird text "+i);
			}
		});
	}

	@Test
	public void testListInit(){
		assertTrue(list.isEmpty());
		assertEquals(list.size(), 0);
	}

	@Test
	public void testAddElements(){
		list.add(0, "Karol");
		list.add(1, "Vanessa");
		list.add(2, "Amanda");

		assertEquals("Karol", list.get(0));
		assertEquals("Vanessa", list.get(1));
		assertEquals("Amanda", list.get(2));

		list.add(1, "Mariana");

		assertEquals("Karol", list.get(0));
		assertEquals("Mariana", list.get(1));
		assertEquals("Vanessa", list.get(2));
		assertEquals("Amanda", list.get(3));

		assertTrue(list.size()==4);
	}

	@Test
	public void testAddElementNull(){
		list.add(0, null);
	}

	@Test
	public void testSetElementNull(){
		list.add(0, "Kheyla");
		list.set(0, null);
	}

	@Test
	public void testSetElement(){
		list.add(0, "Karol");
		list.add(1, "Vanessa");
		list.add(2, "Amanda");

		list.set(1, "Livia");

		assertEquals("Karol", list.get(0));
		assertEquals("Livia", list.get(1));
		assertEquals("Amanda", list.get(2));
	}

	@Test
	public void testRemoveElement(){
		list.add(0, "Karol");
		list.add(1, "Vanessa");
		list.add(2, "Amanda");

		assertEquals("Amanda", list.remove(2));
		assertEquals(2, list.size());
	}

	@Test
	public void testRemoveWithEmptyList(){
		assertThrows(Exception.class,()->list.remove(0));
	}
}