package com.niton.collections.backed;

import com.niton.memory.direct.managed.Section;
import com.niton.memory.direct.managed.VirtualMemory;
import com.niton.memory.direct.stores.ArrayStore;
import com.niton.memory.direct.DataStore;

import java.io.*;

public class BackedListTest {
	private static DataStore byteStore;

	public static void main(String[] args) throws IOException {
		//blockSize,usedSize,startAddress,endAddress
		File f = new File("cache.dat");
		byteStore = new ArrayStore(8*1024);
		//testSections();

		//testVirtualMemory();
		BackedList<String> s = new BackedList<>(byteStore,false);
		s.add("First");
		s.add("Sekkond");
		s.remove(0);
		System.out.println(s);

	}

	private static void testVirtualMemory() throws IOException {
		System.out.println("Virtual Memory test");
		boolean create = false;
		VirtualMemory vm = new VirtualMemory(byteStore);
		if(create) {
			vm.initIndex(1);
			vm.createSection(10, 3);
			vm.createSection(10, 3);
		}else{
			vm.readIndex();
		}
		//populate(vm);
		System.out.println(vm);
		System.out.println(vm.get(0));
		System.out.println(vm.get(1));
		readAndValidate(vm);
	}

	private static void populate(VirtualMemory vm) throws IOException {
		Section s1;
		Section s2;

		s1 = vm.get(0);
		s2 = vm.get(1);
		DataOutputStream o1 = new DataOutputStream(s1.new DataStoreOutputStream());
		DataOutputStream o2 = new DataOutputStream(s2.new DataStoreOutputStream());

		s2.jump(0);
		for (int i = 0; i < 2 * 1024 / 4; i++) {
			o2.writeInt(i);
		}
		s1.jump(0);
		for (int i = 0; i < 2 * 1024 / 4; i++) {
			o1.writeInt(i);
		}
	}

	private static void readAndValidate(VirtualMemory vm) throws IOException {
		Section s1 = vm.get(0);
		Section s2 = vm.get(1);
		DataInputStream i1 = new DataInputStream(s1.new DataStoreInputStream());
		DataInputStream i2 = new DataInputStream(s2.new DataStoreInputStream());

		s1.jump(0);
		for (int i = 0; i < 2*1024/4; i++) {
			int got;
			if(i != (got = i1.readInt()))
				System.out.println("WRONG VALUE (expected:"+i+", got:"+got+")");
		}
		s2.jump(0);
		for (int i = 0; i < 2*1024/4; i++) {
			int got;
			if(i != (got=i2.readInt()))
				System.out.println("WRONG VALUE (expected:"+i+", got:"+got+")");
		}
	}

	private static void testSections() throws IOException {
		System.out.println("Section Test");
		Section s = new Section(0,byteStore);
		s.init(5,1,16);
		DataOutputStream sout = new DataOutputStream(s.openWritingStream());
		DataInputStream sin = new DataInputStream(s.openReadStream());
		s.jump(0);
		System.out.println("Initial state");
		System.out.println(byteStore);
		System.out.println(s.toString());
		for (int i = 10; i <= 40; i+=10) {
			sout.writeInt(i);
			System.out.println("Wrote "+i);
			System.out.println(byteStore);
			System.out.println(s.toString());
		}
		System.out.println("State after write");
		System.out.println(byteStore);
		System.out.println(s.toString());
		System.out.println("Read");
		s.jump(0);
		for (int i = 0; i < 4; i++) {
			System.out.println(sin.readInt());
		}
	}
}
