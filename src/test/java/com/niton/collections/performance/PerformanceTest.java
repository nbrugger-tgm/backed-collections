package com.niton.collections.performance;

import com.niton.collections.backed.BackedList;
import com.niton.memory.direct.stores.ArrayStore;
import com.niton.memory.direct.stores.FileStore;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PerformanceTest {

	private interface Generator<T>{
		T next();
	}

	public static void main(String[] args) throws FileNotFoundException {
		new PerformanceTest().testSuite(RandomObject::new);
	}

	private<T> void testSuite(Generator<T> g) throws FileNotFoundException {
		File f1 = new File("cache.dat");
		File f2 = new File("optimized.dat");
		ArrayStore store = new ArrayStore(1024*1024*40);
		FileStore fileStore = new FileStore(f1);
		FileStore optimizedFileStore = new FileStore(f2);
		Map<String,List<T>> implementations = new HashMap<>();

		createImplementations(store, fileStore, optimizedFileStore,implementations);

		Map<String,List<Measurement>> results = new HashMap<>();


		for (Map.Entry<String, List<T>> implementation : implementations.entrySet()) {
			results.put(implementation.getKey(),testListImplementation(256,implementation.getValue(),g));
		}

		printResult(results);
	}


	private void printResult(Map<String, List<Measurement>> results) {
		PrettyPrinter printer = new PrettyPrinter(System.out);

		String[][] table = new String[results.size()+1][];

		List<Measurement> res1 = results.values().stream().findFirst().get();
		String[] header = new String[1+res1.size()];
		int i = 1;
		for (String s : res1.stream().map(Measurement::getName).toArray(String[]::new)) {
			header[i++] = s;
		}

		table[0] = header;
		i = 1;
		for (Map.Entry<String, List<Measurement>> result : results.entrySet()) {
			String[] row = new String[result.getValue().size()+1];
			row[0] = result.getKey();
			final int[] j = {0};
			result.getValue().forEach(e -> row[++j[0]] = Long.toString(e.getMs()));
			table[i++] = row;
		}
		printer.print(table);
	}

	private <T> void createImplementations(ArrayStore store, FileStore fileStore,FileStore optimized, Map<String, List<T>> implementations) {
		BackedList<T> allOptimizedArrayStoreList = new BackedList<>(store, false);
		allOptimizedArrayStoreList.setIncrementSize(1024*10);
		allOptimizedArrayStoreList.reservedObjectSpace = 200;
		implementations.put("All optimized ArrayStoreList", allOptimizedArrayStoreList);

		BackedList<T> optimizedIncrementArrayStoreList = new BackedList<>(store, false);
		optimizedIncrementArrayStoreList.setIncrementSize(1024*10);
		implementations.put("Increment Optimized ArrayStoreList",optimizedIncrementArrayStoreList);

		BackedList<T> optimizedObjectReserveArrayStoreList = new BackedList<>(store, false);
		optimizedObjectReserveArrayStoreList.reservedObjectSpace = 200;
		implementations.put("ObjectSpace optimized ArrayStoreList",optimizedObjectReserveArrayStoreList);
		implementations.put("Array Store backed list",new BackedList<>(store,false));

		implementations.put("File backed List",new BackedList<>(fileStore,false));

		BackedList<T> optimizedFileStore = new BackedList<>(optimized,false);
		optimizedFileStore.reservedObjectSpace = 200;
		optimizedFileStore.setIncrementSize(512);
		implementations.put("Optimized File Store",optimizedFileStore);

		implementations.put("ArrayList",new ArrayList<>());
		implementations.put("LinkedList",new LinkedList<>());
	}

	private <T> List<Measurement> testListImplementation(int elements, List<T> implementation, Generator<T> gen){
		return Arrays.asList(
				testListZeroInsertAdding(elements,gen,implementation),
				clear(implementation),
				testListInsertAdding(elements,gen,implementation),
				indexLooping(implementation),
				foreach(implementation),
				iterator(implementation),
				randomAccess(implementation),
				testListAdding(elements,gen,implementation),
				removeAllFrom0(implementation)
		);
	}

	public <T> Measurement testListAdding(int elements, Generator<T> gen, List<T> implementation){
		return measure("Insert at End", ()->{
			for (int i = 0; i < elements; i++) {
				T t = gen.next();
				implementation.add(t);
			}
		});
	}
	public <T> Measurement testListInsertAdding(int elements, Generator<T> gen, List<T> implementation){
		return measure("Insert at random", ()->{
			for (int i = 0; i < elements; i++) {
				T t = gen.next();
				implementation.add((int) (Math.random()*implementation.size()),t);
			}
		});
	}
	public <T> Measurement testListZeroInsertAdding(int elements, Generator<T> gen, List<T> implementation){
		return measure("Insert at 0",()->{
			for (int i = 0; i < elements; i++) {
				T t = gen.next();
				implementation.add(t);
			}
		});
	}
	public <T> Measurement clear(List<T> implementation){
		return measure("clear", implementation::clear);
	}
	public <T> Measurement foreach(List<T> implementation){
		return measure("forach",()->{
			boolean b = false;
			for(T t : implementation){
				b |= t.equals(System.currentTimeMillis());
			}
		});
	}
	public <T> Measurement iterator(List<T> implementation){
		return measure("iterator",()->{
			Iterator<T> it = implementation.listIterator();
			while(it.hasNext())
				it.next();
		});
	}
	public <T> Measurement indexLooping(List<T> implementation){
		return measure("index looping",()->{
			for (int i = 0; i < implementation.size(); i++) {
				T t = implementation.get(i);
			}
		});
	}
	public <T> Measurement removeAllFrom0(List<T> implementation){
		return measure("remove all from 0",()->{
			while (implementation.size()>0) {
				implementation.remove(0);
			}
		});
	}
	public <T> Measurement randomAccess(List<T> implementation){
		return measure("Random access",()->{
			for (int i = 0; i < implementation.size(); i++) {
				T t = implementation.get((int) (Math.random()*implementation.size()));
			}
		});
	}

	private static class Measurement {
		private final long ms;
		private final String name;

		private Measurement(long ms, String name) {
			this.ms = ms;
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public long getMs() {
			return ms;
		}
	}

	public Measurement measure(String s, Runnable r){
		long completeStart = System.currentTimeMillis();
		r.run();
		return new Measurement(System.currentTimeMillis() - completeStart, s);
	}
	private static final class PrettyPrinter {

		private static final char BORDER_KNOT = '+';
		private static final char HORIZONTAL_BORDER = '-';
		private static final char VERTICAL_BORDER = '|';

		private static final String DEFAULT_AS_NULL = "(NULL)";

		private final PrintStream out;
		private final String asNull;

		public PrettyPrinter(PrintStream out) {
			this(out, DEFAULT_AS_NULL);
		}

		public PrettyPrinter(PrintStream out, String asNull) {
			if ( out == null ) {
				throw new IllegalArgumentException("No print stream provided");
			}
			if ( asNull == null ) {
				throw new IllegalArgumentException("No NULL-value placeholder provided");
			}
			this.out = out;
			this.asNull = asNull;
		}

		public void print(String[][] table) {
			if ( table == null ) {
				throw new IllegalArgumentException("No tabular data provided");
			}
			if ( table.length == 0 ) {
				return;
			}
			final int[] widths = new int[getMaxColumns(table)];
			adjustColumnWidths(table, widths);
			printPreparedTable(table, widths, getHorizontalBorder(widths));
		}

		private void printPreparedTable(String[][] table, int widths[], String horizontalBorder) {
			final int lineLength = horizontalBorder.length();
			out.println(horizontalBorder);
			for ( final String[] row : table ) {
				if ( row != null ) {
					out.println(getRow(row, widths, lineLength));
					out.println(horizontalBorder);
				}
			}
		}

		private String getRow(String[] row, int[] widths, int lineLength) {
			final StringBuilder builder = new StringBuilder(lineLength).append(VERTICAL_BORDER);
			final int maxWidths = widths.length;
			for ( int i = 0; i < maxWidths; i++ ) {
				builder.append(padRight(getCellValue(safeGet(row, i, null)), widths[i])).append(VERTICAL_BORDER);
			}
			return builder.toString();
		}

		private String getHorizontalBorder(int[] widths) {
			final StringBuilder builder = new StringBuilder(256);
			builder.append(BORDER_KNOT);
			for ( final int w : widths ) {
				for ( int i = 0; i < w; i++ ) {
					builder.append(HORIZONTAL_BORDER);
				}
				builder.append(BORDER_KNOT);
			}
			return builder.toString();
		}

		private int getMaxColumns(String[][] rows) {
			int max = 0;
			for ( final String[] row : rows ) {
				if ( row != null && row.length > max ) {
					max = row.length;
				}
			}
			return max;
		}

		private void adjustColumnWidths(String[][] rows, int[] widths) {
			for ( final String[] row : rows ) {
				if ( row != null ) {
					for ( int c = 0; c < widths.length; c++ ) {
						final String cv = getCellValue(safeGet(row, c, asNull));
						final int l = cv.length();
						if ( widths[c] < l ) {
							widths[c] = l;
						}
					}
				}
			}
		}

		private static String padRight(String s, int n) {
			return String.format("%1$-" + n + "s", s);
		}

		private static String safeGet(String[] array, int index, String defaultValue) {
			return index < array.length ? array[index] : defaultValue;
		}

		private String getCellValue(Object value) {
			return value == null ? asNull : value.toString();
		}

	}

	public static class RandomObject implements Serializable {
		private int x = (int) (Math.random()*200),y = (int) (Math.random()*2000),z= (int) (900*Math.random());

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof RandomObject)) return false;

			RandomObject that = (RandomObject) o;

			if (x != that.x) return false;
			if (y != that.y) return false;
			return z == that.z;
		}

		@Override
		public int hashCode() {
			int result = x;
			result = 31 * result + y;
			result = 31 * result + z;
			return result;
		}
	}
	public static class NonRandomObject implements Serializable {
		int x = 200,y = 900,z = 2000;
	}
}
