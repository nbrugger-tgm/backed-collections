package com.niton.collections.backed.stores;

import static org.junit.jupiter.api.Assertions.*;

class ArrayStoreTest extends DataStoreTest{

	@Override
	protected DataStore createDataStoreImpl() {
		return new ArrayStore(2049);
	}
}