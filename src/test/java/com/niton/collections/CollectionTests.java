package com.niton.collections;

import com.google.common.collect.testing.ListTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.SetTestSuiteBuilder;
import com.google.common.collect.testing.TestListGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.ListFeature;
import com.niton.collections.backed.BackedList;
import com.niton.memory.direct.stores.ArrayStore;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.w3c.dom.css.Rect;

import java.awt.*;
import java.util.List;

public class CollectionTests {
	private final ArrayStore memory = new ArrayStore(10*1024*1024);
	public static Test suite() {
		return new CollectionTests().allTests();
	}

	public junit.framework.Test allTests() {
		TestSuite suite =
				new TestSuite("package.name.of.MySetTest");
		suite.addTest(testBackedList());
		//suite.addTest(testBackedMap());
		return suite;
	}
	public Test testBackedList() {
		return ListTestSuiteBuilder
				.using(new TestListGenerator<Rectangle>() {
					@Override
					public SampleElements<Rectangle> samples() {
						return new SampleElements<>(
								new Rectangle(1,2,3,4),
								new Rectangle(3,4,5,6),
								new Rectangle(6,3,2,1),
								new Rectangle(2000,3000,4000,5000),
								new Rectangle()
								);
					}

					@Override
					public List<Rectangle> create(Object... elements) {
						List<Rectangle> lst = new BackedList<>(memory,false);
						for(Object o : elements)
							lst.add((Rectangle) o);
						return lst;
					}

					@Override
					public Rectangle[] createArray(int length) {
						return new Rectangle[length];
					}

					@Override
					public Iterable<Rectangle> order(List<Rectangle> insertionOrder) {
						return insertionOrder;
					}
				})
				.named("In Memory Backed List")
				.withFeatures(
					CollectionSize.ANY,
					CollectionFeature.ALLOWS_NULL_VALUES,
					CollectionFeature.ALLOWS_NULL_QUERIES,
					CollectionFeature.KNOWN_ORDER,
					CollectionFeature.SUPPORTS_ADD,
					CollectionFeature.SUPPORTS_REMOVE,
					ListFeature.SUPPORTS_SET,
					ListFeature.SUPPORTS_ADD_WITH_INDEX,
					ListFeature.SUPPORTS_REMOVE_WITH_INDEX
				)
				.createTestSuite();
	}
}
