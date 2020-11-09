package com.niton;

import com.niton.collections.backed.BackedPerformanceList;
import com.niton.collections.backed.Serializer;
import com.niton.memory.direct.stores.FileStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ArbitraryCodeRunner {
	public static void main(String[] args) throws FileNotFoundException {
		File f = new File("Weirdmap.dat");
		FileStore store = new FileStore(f);
		Stream.generate(() -> (byte)(Math.random()*Integer.MAX_VALUE)).limit(4*1024*1024).forEach(store::write);

		BackedPerformanceList<String> lst = new BackedPerformanceList<>(store, Serializer.STRING, false);
		lst.add("Es");
		lst.remove("Es");
		lst.add("Du");
		lst.add("Er");
		lst.add(1,"Es");

		List<String> all = Arrays.asList("Es","Du","Er");
		lst.removeAll(all);

		System.out.println(all);
	}
}
