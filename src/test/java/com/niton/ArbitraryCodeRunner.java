package com.niton;

import com.niton.collections.backed.BackedMap;
import com.niton.collections.backed.BackedPerformanceList;
import com.niton.collections.backed.Serializer;
import com.niton.memory.direct.stores.FileStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ArbitraryCodeRunner {
	public static void main(String[] args) throws FileNotFoundException {
		File f = new File("Weirdmap.dat");
		FileStore store = new FileStore(f);
		Stream.generate(() -> (byte)(Math.random()*Integer.MAX_VALUE)).limit(4*1024*1024).forEach(store::write);
		BackedPerformanceList<String> lst = new BackedPerformanceList<>(store,false,Serializer.STRING);
		lst.add("Es");
		lst.add("Du");
		lst.add("Er");
		lst.add(1,"Es");
		System.out.println(lst);
	}
}
