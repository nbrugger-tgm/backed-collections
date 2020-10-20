package com.niton.memory.direct.stores;

import com.niton.memory.direct.DataStore;

class ArrayStoreTest extends DataStoreTest{

	@Override
	protected DataStore createDataStoreImpl() {
		return new ArrayStore(2049);
	}
}