package com.niton.collections.backed;

import java.io.*;
import java.util.*;

import com.niton.StorageException;
import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.managed.*;

public class BackedMap<K,V> extends AbstractMap<K,V> {
	/**
	 * This section holds all the KEY HASHES and maps them to an int which is representing the count of keys associated for that hash
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
			keySegment;
	public int KEY_SIZE_ALLOC = 128;
	public int VALUE_SIZE_ALLOC = 512;
	public final static int KEY_HASH_PAIR_SIZE = 8+4;

	public BackedMap(DataStore mainMemory, boolean read) {
		this(mainMemory,new OOSSerializer<>(),new OOSSerializer<>(),read);
	}

	public BackedMap(DataStore mainMemory, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean read) {
		this.mainMemory = new VirtualMemory(mainMemory,BitSystem.x32);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		if(read) {
			this.mainMemory.readIndex();
			keyHashes = this.mainMemory.get(0);
		}else{
			//Create Memory Structure
			this.mainMemory.initIndex(3);
			keyHashes = this.mainMemory.createSection(KEY_HASH_PAIR_SIZE, 512);
			this.mainMemory.createSection(VALUE_SIZE_ALLOC+keyHashes.getHeaderSize(),512);//VALUE_SEGMENT
			this.mainMemory.createSection(KEY_SIZE_ALLOC+keyHashes.getHeaderSize(), 512); //KEY_SEGMENT
		}
		dataSegment = new VirtualMemory(this.mainMemory.get(1));
		keySegment = new VirtualMemory(this.mainMemory.get(2));
		if(!read){
			//16 can be tweaked for performance
			dataSegment.initIndex(128);
			keySegment.initIndex(128);
		}else{
			dataSegment.readIndex();
			keySegment.readIndex();
		}
	}

	private final VirtualMemory mainMemory;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;


	public BackedMap<K, V> setKEY_SIZE_ALLOC(int KEY_SIZE_ALLOC) {
		this.KEY_SIZE_ALLOC = KEY_SIZE_ALLOC;
		return this;
	}

	public BackedMap<K, V> setVALUE_SIZE_ALLOC(int VALUE_SIZE_ALLOC) {
		this.VALUE_SIZE_ALLOC = VALUE_SIZE_ALLOC;
		return this;
	}

	@Override
	public int size() {
		try {
			int s = 0;
			DataInputStream dis = new DataInputStream(keyHashes.openReadStream());
			keyHashes.jump(0);
			int pools = getHashPoolCount();
			for(int i = 0; i< pools; i++){
				keyHashes.skip(8);
				long poolSize = dis.readInt();
				s += poolSize;
			}
			return s;
		}catch (IOException e){
			throw new StorageException(e);
		}
	}

	private int getHashPoolCount() {
		return (int) (keyHashes.size()/KEY_HASH_PAIR_SIZE);
	}

	@Override
	public V put(K key, V value) {
		if(containsKey(key)){
			V old = get(key);
			dataSegment.get(getKeyIndex(key)).write(value,valueSerializer);
			return old;
		}else{
			addEntry(key, value);
			return null;
		}
	}

	private void addEntry(K key, V value) {
		long keyHash = key == null ? 0 : key.hashCode();
		int[] poolInfo = getHashPoolInfo(keyHash);

		//test hash-pool existence
		if(poolInfo.length == 0){
			createHashPool(keyHash,1);
			poolInfo = getHashPoolInfo(keyHash);
		}else{
			alterHashPoolSize(poolInfo[0],1);
			poolInfo[2] += 1;
		}

		Section keyStore = keySegment.insertSection(poolInfo[2],KEY_SIZE_ALLOC / 4, 4);
		Section valueStore = dataSegment.insertSection(poolInfo[2],VALUE_SIZE_ALLOC / 4, 4);
		keyStore.write(key,keySerializer);
		valueStore.write(value,valueSerializer);
	}

	private void alterHashPoolSize(int address, int enlargement) {
		DataInputStream dis = new DataInputStream(keyHashes.openReadStream());
		DataOutputStream dos = new DataOutputStream(keyHashes.openWritingStream());
		keyHashes.jump(address+8);
		try {
			int old = dis.readInt();
			keyHashes.jump(address+8);
			dos.writeInt(old+enlargement);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	private void createHashPool(long hash,int size) {
		DataOutputStream dos = new DataOutputStream(keyHashes.openWritingStream());
		keyHashes.jumpToEnd();
		try {
			dos.writeLong(hash);
			dos.writeInt(size);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	private int getKeyIndex(Object key) {
		int[] info = getHashPoolInfo(key == null ? 0 : key.hashCode());
		if(info.length == 0)
			return -1;
		//addr,start,end
		if(info[1] == info[2])
			return info[1];
		for (int i = info[1]; i <= info[2]; i++) {
			K curr = keySegment.get(i).read(keySerializer);
			if(Objects.equals(curr, key))
				return i;
		}
		return -1;
	}

	/**
	 * @param hash
	 * @return array of infos
	 * <code>
	 *      [0] = HashPool Addresses
	 *      [1] = Pool Start Index
	 *      [2] = pool End Index
	 * </code>
	 */
	private int[] getHashPoolInfo(long hash) {
		int from = 0;
		int pools = getHashPoolCount();
		DataInputStream poolReader = new DataInputStream(keyHashes.openReadStream());
		keyHashes.jump(0);
		try {
			for (int i = 0; i < pools; i++) {
				long poolHash = poolReader.readLong();
				int size = poolReader.readInt();
				if(poolHash == hash){
					return new int[]{i*KEY_HASH_PAIR_SIZE,from,from+size-1};
				}else{
					from += size;
				}
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
		return new int[0];
	}


	@Override
	public V get(Object key) {
		long i = getKeyIndex(key);
		if(i==-1)
			return null;
		else return dataSegment.get(i).read(valueSerializer);
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
		keyHashes.cut(0);
	}

	@Override
	public V remove(Object key) {
		V val = get(key);
		long hash = key == null ? 0: key.hashCode();
		int[] poolInfo = getHashPoolInfo(hash);
		int poolIndex = getKeyIndex(key);
		if(poolIndex == -1)
			return null;
		dataSegment.deleteSegment(poolIndex);
		keySegment.deleteSegment(poolIndex);
		alterHashPoolSize(poolInfo[0],-1);
		return val;
	}
	private interface IteratorSupplier<T>{
		Iterator<T> newIterator();
	}
	private class IteratorSet<T> extends AbstractSet<T> {

		@Override
		public boolean retainAll(Collection<?> c) {
			if(c == null)
				throw new NullPointerException();
			if(c.size() == 0){
				//empty retain clears map
				int old = BackedMap.this.size();
				BackedMap.this.clear();
				return old > BackedMap.this.size();
			}

			boolean result = false;
			Iterator<T> iter = iterator();
			while(iter.hasNext()) {
				T kvEntry = iter.next();
				if(c.contains(kvEntry))
					continue;
				iter.remove();
				result = true;
			}
			return result;
		}

		private final IteratorSupplier<T> iter;

		private IteratorSet(IteratorSupplier<T> iter) {
			this.iter = iter;
		}

		@Override
		public boolean remove(Object o) {
			if(o instanceof Entry) {
				boolean ret = BackedMap.this.containsKey(((Entry<?, ?>) o).getKey());
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

		@Override
		public String toString() {
			return String.format("%s=%s", key,valueCache);
		}
	}
}
