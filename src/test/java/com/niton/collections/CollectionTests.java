package com.niton.collections;

import com.google.common.collect.testing.*;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.ListFeature;
import com.google.common.collect.testing.features.MapFeature;
import com.niton.collections.backed.BackedList;
import com.niton.collections.backed.BackedMap;
import com.niton.collections.backed.Serializer;
import com.niton.collections.backed.SimpleBackedList;
import com.niton.memory.direct.stores.ArrayStore;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.w3c.dom.css.Rect;

import java.awt.*;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.collect.testing.features.CollectionFeature.*;
import static com.google.common.collect.testing.features.MapFeature.*;
import static com.google.common.collect.testing.features.MapFeature.ALLOWS_NULL_VALUES;
import static com.google.common.collect.testing.features.MapFeature.GENERAL_PURPOSE;
import static com.google.common.collect.testing.features.MapFeature.SUPPORTS_REMOVE;

public class CollectionTests {
	private final ArrayStore memory = new ArrayStore(10*1024*1024);
	public static Test suite() {
		return new CollectionTests().allTests();
	}

	public junit.framework.Test allTests() {
		try {
		TestSuite suite =
				new TestSuite("com.niton.collection.backed");
			suite.addTest(testBackedList());
			suite.addTest(testBackedMap());
			return suite;
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			return null;
		}
	}

	private Test testBackedMap() {
		return MapTestSuiteBuilder
				.using(new TestMapGenerator<String, String>() {
					@Override
					public SampleElements<Map.Entry<String, String>> samples() {
						return new SampleElements<>(
								new AbstractMap.SimpleEntry<>("Nils","Brugger"),
								new AbstractMap.SimpleEntry<>("Anna", "Brugger"),
								new AbstractMap.SimpleEntry<>("Bernhard","Hickel"),
								new AbstractMap.SimpleEntry<>("Ilse","szabo"),
								new AbstractMap.SimpleEntry<>("Jonny","Waiza")
						);
					}

					@Override
					public Map<String, String> create(Object... elements) {
						BackedMap<String,String> create = new BackedMap<>(memory,Serializer.STRING,Serializer.STRING,false);
						for (Object element : elements) {
							Entry<String,String> e = (Map.Entry<String, String>) element;
							create.put(e.getKey(),e.getValue());
						}
						return create;
					}

					@Override
					public Map.Entry<String, String>[] createArray(int length) {
						return new Map.Entry[length];
					}

					@Override
					public Iterable<Map.Entry<String, String>> order(List<Map.Entry<String, String>> insertionOrder) {
						return insertionOrder;
					}

					@Override
					public String[] createKeyArray(int length) {
						return new String[length];
					}

					@Override
					public String[] createValueArray(int length) {
						return new String[length];
					}
				})
				.named("Backed Map")
				.withFeatures(
						CollectionSize.ANY,
						SUPPORTS_REMOVE,
						GENERAL_PURPOSE,
						ALLOWS_ANY_NULL_QUERIES,
						ALLOWS_NULL_VALUES,
						ALLOWS_NULL_KEYS,
						SUPPORTS_PUT
				).createTestSuite();
	}

	public Test testBackedList() throws NoSuchMethodException {
		return ListTestSuiteBuilder
				.using(new TestListGenerator<String>() {
					@Override
					public SampleElements<String> samples() {
						return new SampleElements<>(
							"Ich",
							"Du",
							"Er",
							"Sie",
							"Es"
						);
					}

					@Override
					public List<String> create(Object... elements) {
						BackedList<String> lst = new BackedList<>(memory, Serializer.STRING,false);
						for(Object o : elements)
							lst.add((String) o);
						return lst;
					}

					@Override
					public String[] createArray(int length) {
						return new String[length];
					}

					@Override
					public Iterable<String> order(List<String> insertionOrder) {
						return insertionOrder;
					}
				})
				.suppressing(BackedList.class.getMethod("iterator"),BackedList.class.getMethod("listIterator"),BackedList.class.getMethod("listIterator"))
				.named("In Memory Backed List")
				.withFeatures(
					CollectionSize.ANY,
					CollectionFeature.ALLOWS_NULL_VALUES,
					CollectionFeature.ALLOWS_NULL_QUERIES,
					CollectionFeature.KNOWN_ORDER,
					SUPPORTS_ADD,
					SUPPORTS_REMOVE,
					ListFeature.SUPPORTS_SET,
					ListFeature.SUPPORTS_ADD_WITH_INDEX,
					ListFeature.SUPPORTS_REMOVE_WITH_INDEX
				)
				.createTestSuite();
	}
}
