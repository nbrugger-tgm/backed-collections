package com.niton.collections.backed;

import com.niton.collections.backed.stores.ArrayStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class BackedListTest {
	public static void main(String[] args) {
		ArrayStore byteStore = new ArrayStore(200);
		BackedList<Byte> test = new BackedList<>(byteStore, new Serializer<Byte>() {
			@Override
			public void write(Byte data, OutputStream store) throws IOException {
				store.write(data-Byte.MIN_VALUE);
			}

			@Override
			public Byte read(InputStream store) throws IOException, ClassNotFoundException {
				return (byte)(store.read()+ Byte.MIN_VALUE);
			}
		});
		Arrays.stream(byteStore.getClass().getDeclaredFields()).forEach(e -> System.out.println(e.getName()));
//		System.out.println(byteStore);
//		test.add((byte) 1);
//		System.out.println(byteStore);
//		test.add((byte) 2);
//		System.out.println(byteStore);
//		test.add((byte) 3);
//		System.out.println(byteStore);
//		test.add((byte) 4);
//		System.out.println(byteStore);
//
//
//		ArrayStore store = new ArrayStore(1024*1024);
//		BackedList<String> list = new BackedList<>(store);
//		for (int i = 0; i < 1000; i++) {
//			list.add("Some string");
//		}
//		list.forEach(System.out::println);
	}
}
