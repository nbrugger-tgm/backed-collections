package com.niton.memory.direct.stores;

class ArrayStoreTest extends DataStoreTest{

	@Override
	protected DataStore createDataStoreImpl() {
		return new ArrayStore(2049);
	}
}