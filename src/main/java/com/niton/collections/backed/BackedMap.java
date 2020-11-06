package com.niton.collections.backed;

import com.niton.StorageException;
import com.niton.collections.BaseCollection;
import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.managed.BitSystem;
import com.niton.memory.direct.managed.Section;
import com.niton.memory.direct.managed.VirtualMemory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class BackedMap<K,V> implements Map<K,V> {
	public final static int KEY_HASH_PAIR_SIZE = 8+4;
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
	private final VirtualMemory mainMemory;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;
	private final HashMap<Long,int[]> poolInfoCache = new HashMap<>();
	public int KEY_SIZE_ALLOC = 128;
	public int VALUE_SIZE_ALLOC = 512;
	public boolean useSizeCaching = true;
	public boolean useCaching = false;
	private boolean sizeCached = false;
	private int sizeCache = 0;
	private boolean replaced = false;
	public BackedMap(DataStore mainMemory, boolean read) {
		this(mainMemory,new OOSSerializer<>(),new OOSSerializer<>(),read);
	}

	public BackedMap(DataStore mainMemory, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean read) {
		this.mainMemory = new VirtualMemory(mainMemory,BitSystem.X32);
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

	public BackedMap<K, V> setKEY_SIZE_ALLOC(int KEY_SIZE_ALLOC) {
		this.KEY_SIZE_ALLOC = KEY_SIZE_ALLOC;
		return this;
	}

	public BackedMap<K, V> setVALUE_SIZE_ALLOC(int VALUE_SIZE_ALLOC) {
		this.VALUE_SIZE_ALLOC = VALUE_SIZE_ALLOC;
		return this;
	}
	@Override
	public boolean equals(Object obj) {
		return obj != null && obj instanceof Map && ((Map<?, ?>) obj).size() == size() && entrySet().containsAll(((Map<?, ?>) obj).entrySet());
	}


	@Override
	public int size() {
		if(useSizeCaching && sizeCached)
			return sizeCache;
		try {
			int s = 0;
			DataInputStream dis = new DataInputStream(new BufferedInputStream(keyHashes.openReadStream(),4));
			keyHashes.jump(0);
			int pools = getHashPoolCount();
			for(int i = 0; i< pools; i++){
				keyHashes.skip(8);
				long poolSize = dis.readInt();
				s += poolSize;
			}
			sizeCache = s;
			sizeCached = true;
			return s;
		}catch (IOException e){
			throw new StorageException(e);
		}
	}

	@Override
	public boolean isEmpty() {
		return size()==0;
	}

	private int getHashPoolCount() {
		return (int) (keyHashes.size()/KEY_HASH_PAIR_SIZE);
	}

	@Override
	public V replace(K key, V value) {
		int index = getKeyIndex(key);
		if(index != -1){
			replaced = true;
			Section valueSection = dataSegment.get(index);
			V old = valueSection.read(valueSerializer);
			valueSection.write(value,valueSerializer);
			return old;
		}
		return null;
	}

	@Override
	public V put(K key, V value) {
		replaced = false;
		V v = replace(key,value);
		if(!replaced){
			addEntry(key, value);
			return null;
		}
		return v;
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
			if(!useCaching)
				poolInfo[2] += 1;
		}

		Section keyStore = keySegment.insertSection(poolInfo[2],KEY_SIZE_ALLOC / 4, 4);
		Section valueStore = dataSegment.insertSection(poolInfo[2],VALUE_SIZE_ALLOC / 4, 4);
		keyStore.write(key,keySerializer);
		valueStore.write(value,valueSerializer);
		sizeCache++;
	}

	@Override
	public Collection<V> values() {
		return new ValueCollection();
	}

	@Override
	public boolean containsKey(Object key) {
		return getKeyIndex(key)!=-1;
	}

	@Override
	public boolean containsValue(Object value) {
		return values().contains(value);
	}

	private void alterHashPoolSize(int address, int enlargement) {
		DataInputStream dis = new DataInputStream(new BufferedInputStream(keyHashes.openReadStream(),12));
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(keyHashes.openWritingStream(),4));
		keyHashes.jump(address);
		try {
			long hash = dis.readLong();
			int old = dis.readInt();
			keyHashes.jump(address+8);
			dos.writeInt(old+enlargement);
			dos.flush();
			poolInfoCache.get(hash)[2] += enlargement;
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	private void createHashPool(long hash,int size) {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(keyHashes.openWritingStream(),12));
		keyHashes.jumpToEnd();
		try {
			dos.writeLong(hash);
			dos.writeInt(size);
			dos.flush();
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

		byte[] serial;
		try {
			serial = keySerializer.serialize((K) key);
		}catch (ClassCastException castException){
			return -1;
		}

		for (int i = info[1]; i <= info[2]; i++) {
			if(Arrays.equals(keySegment.get(i).read(0,serial.length),serial))
				//if(Objects.equals(key,keySegment.get(i).read(keySerializer)))
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
		if(useCaching && poolInfoCache.containsKey(hash))
			return poolInfoCache.get(hash);
		int from = 0;
		int pools = getHashPoolCount();
		DataInputStream poolReader = new DataInputStream(new BufferedInputStream(keyHashes.openReadStream(),4));
		keyHashes.jump(0);
		try {
			for (int i = 0; i < pools; i++) {
				long poolHash = poolReader.readLong();
				int size = poolReader.readInt();
				if(poolHash == hash){
					int[] info = new int[]{i*KEY_HASH_PAIR_SIZE,from,from+size-1};
					poolInfoCache.put(hash,Arrays.copyOf(info,info.length));
					return info;
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
		poolInfoCache.clear();
		sizeCache = 0;
	}

	@Override
	public V remove(Object key) {
		V val = get(key);
		long hash = key == null ? 0: key.hashCode();
		int keyIndex = getKeyIndex(hash);
		if(keyIndex == -1)
			return null;
		remove(hash,keyIndex);
		return val;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
			put(entry.getKey(),entry.getValue());
		}
	}

	public void remove(long hash,int index){
		int[] poolInfo = getHashPoolInfo(hash);
		dataSegment.deleteSegment(index);
		keySegment.deleteSegment(index);
		sizeCache--;
		alterHashPoolSize(poolInfo[0],-1);
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
			Iterator<T> removeIterator = iterator();
			while(removeIterator.hasNext()) {
				T kvEntry = removeIterator.next();
				if(c.contains(kvEntry))
					continue;
				removeIterator.remove();
				result = true;
			}
			return result;
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

	private class ValueIterator implements Iterator<V> {
		int keyIndex = 0;
		boolean allowed = false;

		@Override
		public boolean hasNext() {
			return keyIndex <size();
		}

		@Override
		public V next() {
			if(!hasNext())
				throw new NoSuchElementException();
			V key = dataSegment.get(this.keyIndex++).read(valueSerializer);
			allowed = true;
			return key;
		}

		@Override
		public void remove() {
			if(!allowed)
				throw new IllegalStateException("only allowed after next()");
			keyIndex--;
			K key = keySegment.get(keyIndex).read(keySerializer);
			BackedMap.this.remove(key.hashCode(),keyIndex);
			allowed = false;
		}
	}

	private class KeyIterator implements Iterator<K> {
		int keyIndex = 0;
		boolean allowed = false;

		@Override
		public boolean hasNext() {
			return keyIndex <size();
		}

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
			K key = keySegment.get(keyIndex).read(keySerializer);
			BackedMap.this.remove(key == null ? 0: key.hashCode(),keyIndex);
			allowed = false;
		}
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("{");
		sb.append(entrySet().stream().map(e->(e.getKey() == null ? "null" : e.getKey().toString())+"="+(e.getValue() == null ? "null" :e.getValue().toString())).collect(Collectors.joining(", ")));
		sb.append('}');
		return sb.toString();
	}

	@Override
	public int hashCode() {
		int expectedHashCode = 0;
		for (Entry<K, V> entry : entrySet()) {
			expectedHashCode += Objects.hashCode(entry);
		}
		return expectedHashCode;
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

		public boolean equals(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			Map.Entry<?,?> e = (Map.Entry<?,?>)o;
			return Objects.equals(key, e.getKey()) && Objects.equals(valueCache, e.getValue());
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

	private class ValueCollection extends BaseCollection<V> {


		@Override
		public int size() {
			return BackedMap.this.size();
		}

		@Override
		public boolean isEmpty() {
			return BackedMap.this.isEmpty();
		}

		@Override
		public boolean contains(Object o) {
			return indexOf(o)!=-1;
		}
		public int indexOf(Object o) {
			byte[] serial;
			try {
				serial = valueSerializer.serialize((V) o);
			}catch (ClassCastException castException){
				return -1;
			}
			int sz = size();
			for (int i = 0; i < sz; i++) {
				if(Arrays.equals(dataSegment.get(i).read(0,serial.length),serial))
					//if(Objects.equals(key,keySegment.get(i).read(keySerializer)))
					return i;
			}
			return -1;
		}


		@Override
		public Iterator<V> iterator() {
			return new ValueIterator();
		}



		@Override
		public boolean add(V v) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object o) {
			int i = indexOf(o);
			if(i == -1)
				return false;
			o = keySegment.get(i).read(keySerializer);
			int hash = o == null ? 0:o.hashCode();
			BackedMap.this.remove(hash,i);
			return true;
		}



		@Override
		public boolean addAll(Collection<? extends V> c) {
			throw new UnsupportedOperationException();
		}


		@Override
		public boolean retainAll(Collection<?> c) {
			int i = 0;
			boolean changed = false;
			while(i<size()) {
				Object o = get(i);
				if (!c.contains(o)) {
					o = keySegment.get(i).read(keySerializer);
					BackedMap.this.remove(o == null ? 0L:o.hashCode(),i);
					i--;
					changed = true;
				}
				i++;
			}

			return changed;
		}

		protected V get(int i){
			return dataSegment.get(i).read(valueSerializer);
		}

		@Override
		public void clear() {
			BackedMap.this.clear();
		}
	}
}
