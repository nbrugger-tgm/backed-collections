package com.niton.collections.backed;

import java.io.*;
import java.util.*;

import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.managed.*;

public class BackedMap<K,V> extends AbstractMap<K,V> {
	private static final long POOL_ADDR_SIZE = 4+4/*KEY_INDEX,VALUE_INDEX*/;
	/**
	 * This section holds all the KEY HASHES and maps them to an int which is representing the index of the pool Segment fot that hash
	 */
	private final Section keyHashes;
	private final VirtualMemory
			/**
			 * Stores each data entry in a regarding section. Only stores the data object
			 */
			dataSegment,
			/**
			 * Stores the key Object values
			 */
			keySegment,
			/**
			 * stores the hash-pools. A hash pool is map of a references to keySegments where the key data is stored and the index of the data segment
			 */
			poolSegment;
	public int KEY_SIZE_ALLOC = 20;
	public int VALUE_SIZE_ALLOC = 128;
	public final static int KEY_HASH_PAIR_SIZE = 8+4;

	public BackedMap(DataStore mem, boolean read) {
		this(mem,new OOSSerializer<>(),new OOSSerializer<>(),read);
	}

	public BackedMap(DataStore mem, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean read) {
		this.mem = new VirtualMemory(mem,BitSystem.x32);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		if(read) {
			this.mem.readIndex();
		}else{
			this.mem.initIndex(128);
			this.mem.createSection(KEY_HASH_PAIR_SIZE, 10);
			this.mem.createSection(VALUE_SIZE_ALLOC,10);//VALUE_SEGMENT
			this.mem.createSection(KEY_SIZE_ALLOC, 10); //KEY_SEGMENT
			this.mem.createSection(POOL_ADDR_SIZE, 10);//POOL_SEGMENT
		}
		keyHashes = this.mem.get(0);
		dataSegment = new VirtualMemory(this.mem.get(1));
		keySegment = new VirtualMemory(this.mem.get(2));
		poolSegment = new VirtualMemory(this.mem.get(3));
		if(!read){
			//16 can be tweaked for performance
			dataSegment.initIndex(16);
			keySegment.initIndex(16);
			poolSegment.initIndex(16);
		}else{
			dataSegment.readIndex();
			keySegment.readIndex();
			poolSegment.readIndex();
		}
	}

	private final VirtualMemory mem;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;

	@Override
	public int size() {
		try {
			int s = 0;
			DataInputStream dis = new DataInputStream(keyHashes.openReadStream());
			keyHashes.jump(0);
			for(int i = 0; i< getHashPoolCount(); i++){
				dis.skip(8);
				long poolIndex = dis.readInt();
				Section pool = poolSegment.get(poolIndex);
				s += pool.size()/POOL_ADDR_SIZE;
			}
			return s;
		}catch (IOException e){
			throw new RuntimeException(e);
		}
	}

	private int getHashPoolCount() {
		return (int) (keyHashes.size()/KEY_HASH_PAIR_SIZE);
	}

	@Override
	public V put(K key, V value) {
		try {
			V old = get(key);
			if(containsKey(key)){
				dataSegment.get(dataIndexOf(key)).write(value,valueSerializer);
			}else {
				long keyHash = key == null ? 0 : key.hashCode();
				Section hashPool = getOrCreateHashPool(keyHash);
				int keyStoreIndex = (int) keySegment.sectionCount();
				Section keyStore = keySegment.createSection(KEY_SIZE_ALLOC / 4, 4);
				int valueStoreIndex = (int) dataSegment.sectionCount();
				Section valueStore = dataSegment.createSection(VALUE_SIZE_ALLOC / 4, 4);
				DataOutputStream hashPoolWriter = new DataOutputStream(hashPool.openWritingStream());
				hashPool.jumpToEnd();
				hashPoolWriter.writeInt(keyStoreIndex);
				hashPoolWriter.writeInt(valueStoreIndex);
				hashPoolWriter.flush();
				hashPoolWriter.close();
				keyStore.write(key,keySerializer);
				valueStore.write(value,valueSerializer);
			}
			return old;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private int dataIndexOf(Object key) {
		try {
			long hash=key == null?0:key.hashCode();
			Section pool = getHashPool(hash);
			if(pool == null)
				return -1;
			DataInputStream poolReader = new DataInputStream(pool.openReadStream());
			long poolEntries = pool.size()/POOL_ADDR_SIZE;
			if(poolEntries == 1) {
				pool.jump(POOL_ADDR_SIZE/2);
				return poolReader.readInt();
			}else {
				for (int i = 0; i < poolEntries; i++) {
					pool.jump(i * POOL_ADDR_SIZE);
					K oneKey = keySerializer.read(keySegment.get(poolReader.readInt()).openReadStream());
					if (Objects.equals(oneKey,key))
						return poolReader.readInt();
				}
				return -1;
			}
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public V get(Object key) {
		long i = dataIndexOf(key);
		if(i==-1)
			return null;
		else return dataSegment.get(i).read(valueSerializer);
	}

	private Section getOrCreateHashPool(long keyHash) {
		Section s = getHashPool(keyHash);
		if(s == null)
			return createHashPool(keyHash);
		else
			return s;
	}
	private Section getHashPool(long keyHash) {
		long i;
		return (i = getHashPoolIndex(keyHash)) == -1 ? null : poolSegment.get(i);
	}

	private Section createHashPool(long keyHash) {
		try {
			long cnt = getHashPoolCount();
			DataOutputStream dos = new DataOutputStream(keyHashes.openWritingStream());
			keyHashes.jumpToEnd();
			dos.writeLong(keyHash);
			dos.writeInt((int)cnt);
			dos.flush();
			dos.close();

			//4 is subject to change for performance
			return poolSegment.createOrGetSection(POOL_ADDR_SIZE, 4,cnt);
		}catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<K> keySet() {
		return new IteratorSet<>(KeyIterator::new);
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new IteratorSet<>(EntryIterator::new);
	}

	@Override
	public void clear() {
		dataSegment.initIndex(16);
		keySegment.initIndex(16);
		poolSegment.initIndex(16);
		keyHashes.cut(0);
	}

	@Override
	public V remove(Object key) {
		try{
			V val = get(key);
			long hash = key == null ? 0: key.hashCode();
			int poolIndex= getHashPoolIndex(hash);
			if(poolIndex == -1)
				return null;
			Section hashPool = poolSegment.get(poolIndex);
			DataInputStream poolReader = new DataInputStream(hashPool.openReadStream());
			int keysWithThisHash = (int) (hashPool.size()/POOL_ADDR_SIZE);
			long keyIndex = 0,valueIndex = 0;
			hashPool.jump(0);
			if(keysWithThisHash > 1){
				for (int i = 0; i < keysWithThisHash; i++) {
					keyIndex = poolReader.readInt();
					Section keyStore = keySegment.get(keyIndex);
					K keyI = keyStore.read(keySerializer);
					if(Objects.equals(key, keyI)){
						valueIndex = poolReader.readInt();
						hashPool.shiftAll(-POOL_ADDR_SIZE);
						if(hashPool.size() == 0) {
							removeHashPool(hash,poolIndex);
						}
						break;
					}else if (i+1 == keysWithThisHash)
						return null;
				}
			}else{
				keyIndex = poolReader.readInt();
				valueIndex = poolReader.readInt();
				removeHashPool(hash,poolIndex);
			}

			keySegment.deleteSegment(keyIndex);
			dataSegment.deleteSegment(valueIndex);
			return val;
		}catch (IOException e){
			throw new RuntimeException(e);
		}
	}

	private void removeHashPool(long hash, int poolIndex) {
		try {
			poolSegment.deleteSegment(poolIndex);
			DataInputStream dis = new DataInputStream(keyHashes.openReadStream());
			int size = getHashPoolCount();
			for (int i = 0; i < size; i++) {
				keyHashes.jump(i*KEY_HASH_PAIR_SIZE);
				long currentHash = dis.readLong();
				if(currentHash == hash) {
					keyHashes.skip(4);
					keyHashes.shiftAll(-KEY_HASH_PAIR_SIZE);
					keyHashes.cut(keyHashes.size()-KEY_HASH_PAIR_SIZE);
					return;
				}
				keyHashes.skip(4);
			}
			throw new RuntimeException("Couldnt find hash link");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	private int getHashPoolIndex(long keyHash) {
		try {
			DataInputStream dis = new DataInputStream(keyHashes.openReadStream());
			int keyPools = getHashPoolCount();
			keyHashes.jump(0);
			for(int i = 0;i<keyPools;i++){
				long hash = dis.readLong();
				int poolIndex = dis.readInt();
				if(hash == keyHash){
					return poolIndex;
				}
			}
			dis.close();
			return -1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	private interface IteratorSupplier<T>{
		Iterator<T> newIterator();
	}
	private class IteratorSet<T> extends AbstractSet<T> {


		private final IteratorSupplier<T> iter;

		private IteratorSet(IteratorSupplier<T> iter) {
			this.iter = iter;
		}

		@Override
		public boolean remove(Object o) {
			if(o instanceof Entry) {
				boolean ret = contains(((Entry<?, ?>) o).getKey());
				BackedMap.this.remove(((Entry<?,?>) o).getKey());
				return ret;
			}
			boolean ret = contains(o);
			BackedMap.this.remove(o);
			return ret;
		}

		@Override
		public void clear() {
			BackedMap.this.clear();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			int sz = BackedMap.this.size();
			for (Object o : c) {
				if(o instanceof Entry) {
					Entry entry = (Entry) o;
					BackedMap.this.remove(entry.getKey());
				}else{
					BackedMap.this.remove(o);
				}
			}

			return sz != BackedMap.this.size();
		}
		@Override
		public Iterator<T> iterator() {
			return iter.newIterator();
		}

		@Override
		public int size() {
			return BackedMap.this.size();
		}
	}
	private class EntryIterator implements Iterator<Entry<K,V>> {
		private Iterator<K> keyIterator = new KeyIterator();
		@Override
		public boolean hasNext() {
			return keyIterator.hasNext();
		}

		@Override
		public Entry<K, V> next() {
			K k = keyIterator.next();
			return new MutableEntry(k);
		}

		@Override
		public void remove() {
			keyIterator.remove();
		}
	}
	private class KeyIterator implements Iterator<K> {
		int keyIndex = 0;
		@Override
		public boolean hasNext() {
			return keyIndex <size();
		}

		boolean allowed = false;

		@Override
		public K next() {
			if(!hasNext())
				throw new NoSuchElementException();
			K key = keySegment.get(this.keyIndex++).read(keySerializer);
			allowed = true;
			return key;
		}

		@Override
		public void remove() {
			if(!allowed)
				throw new IllegalStateException("only allowed after next()");
			keyIndex--;
			BackedMap.this.remove(next());
			keyIndex--;
			allowed = false;
		}
	}

	private K getKey(int hashPool, int posInPool) {
		try {
			Section pool = getHashPool(hashPool);
			if(pool == null)
				return null;
			DataInputStream dis = new DataInputStream(pool.openReadStream());
			pool.jump(posInPool*POOL_ADDR_SIZE);
			Section key = keySegment.get(dis.readInt());
			return key.read(keySerializer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private class MutableEntry implements Entry<K, V> {
		private K key;
		private V valueCache;
		public MutableEntry(K key) {
			this.key = key;
			valueCache = get(key);
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return valueCache;
		}

		@Override
		public V setValue(V value) {
			valueCache = value;
			return put(key,value);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof BackedMap.MutableEntry)) return false;

			MutableEntry that = (MutableEntry) o;

			return Objects.equals(key, that.key);
		}

		@Override
		public int hashCode() {
			return (key == null ? 0 : key.hashCode())^(valueCache == null ? 0 : valueCache.hashCode());
		}
	}
}
