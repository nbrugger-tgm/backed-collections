package com.niton.collections;

import com.niton.collections.backed.BackedList;
import com.niton.collections.backed.Serializer;
import com.niton.memory.direct.stores.ArrayStore;
import com.niton.memory.direct.stores.FixedDataStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BackedListTest {

	@Test
	public void general(){
		BackedList<String> lst = new BackedList<>(new ArrayStore(1024*1024), Serializer.STRING,false);
		lst.add("Wir");
		lst.add("Strings");
		lst.add("sind");
		assertEquals(3,lst.size());
		assertEquals("Strings",lst.get(1));
		lst.remove(1);
		assertEquals(2,lst.size());
		lst.clear();
		assertEquals(0, lst.size());
		assertThrows(Throwable.class, ()->lst.get(0));
		assertThrows(Throwable.class,()->lst.remove(0));
	}

	@Test
	public void testOverflow(){
		BackedList<String> lst = new BackedList<>(new ArrayStore(1024),false);
		assertThrows(FixedDataStore.MemoryOverflowException.class,()->{
			for (int i = 0; i < 10000; i++) {
				lst.add("Some weird text "+i);
			}
		});
	}
}
