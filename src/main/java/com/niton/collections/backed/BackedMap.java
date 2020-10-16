package com.niton.collections.backed;

import java.io.*;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.managed.*;

public class BackedMap<K,V> extends AbstractMap<K,V> {
	private static final long POOL_ADDR_SIZE = 16/*KEY_INDEX,VALUE_INDEX*/;
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
	public int KEY_SIZE_ALLOC = 16;
	public int VALUE_SIZE_ALLOC = 128;
	public final static int KEY_HASH_PAIR_SIZE = 16;

	public BackedMap(DataStore mem, boolean read) {
		this(mem,new OOSSerializer<>(),new OOSSerializer<>(),read);
	}

	public BackedMap(DataStore mem, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean read) {
		this.mem = new VirtualMemory(mem);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		if(read) {
			this.mem.readIndex();
			keyHashes = this.mem.get(0);
		}else{
			this.mem.initIndex(128);
			keyHashes = this.mem.createSection(KEY_HASH_PAIR_SIZE, 10);
			this.mem.createSection(VALUE_SIZE_ALLOC,1);
			this.mem.createSection(KEY_SIZE_ALLOC, 1);
			this.mem.createSection(POOL_ADDR_SIZE, 10);
		}
		dataSegment = new VirtualMemory(this.mem.get(1));
		keySegment = new VirtualMemory(this.mem.get(2));
		poolSegment = new VirtualMemory(this.mem.get(3));
		if(!read){
			//16 can be tweaked for performance
			dataSegment.initIndex(16);
			keySegment.initIndex(16);
			keySegment.initIndex(16);
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
			for(int i = 0;i<getKeyPoolCount();i++){
				dis.skip(8);
				long poolIndex = dis.readLong();
				Section pool = poolSegment.get(poolIndex);
				s += pool.size()/POOL_ADDR_SIZE;
			}
			return s;
		}catch (IOException e){
			throw new RuntimeException(e);
		}
	}

	private int getKeyPoolCount() {
		return (int) (keyHashes.size()/KEY_HASH_PAIR_SIZE);
	}

	@Override
	public V put(K key, V value) {
		try {
			V old = get(key);
			long keyHash = key.hashCode();
			Section hashPool = getOrCreateHashPool(keyHash);
			long keyStoreIndex = keySegment.sectionCount();
			Section keyStore = keySegment.createSection(KEY_SIZE_ALLOC/2,2);
			long valueStoreIndex = dataSegment.sectionCount();
			Section valueStore = dataSegment.createSection(VALUE_SIZE_ALLOC/2,2);
			DataOutputStream hashPoolWriter = new DataOutputStream(hashPool.openWritingStream());
			hashPoolWriter.writeLong(keyStoreIndex);
			hashPoolWriter.writeLong(valueStoreIndex);
			hashPoolWriter.flush();
			hashPoolWriter.close();
			keySerializer.write(key,keyStore.openWritingStream());
			valueSerializer.write(value,valueStore.openWritingStream());
			return old;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public V get(Object key) {
		try {
			long hash=key.hashCode();
			Section pool = getHashPool(hash);
			if(pool == null)
				return null;
			DataInputStream poolReader = new DataInputStream(pool.openReadStream());
			long poolEntries = pool.size()/POOL_ADDR_SIZE;
			if(poolEntries == 1) {
				pool.jump(POOL_ADDR_SIZE/2);
				return valueSerializer.read(dataSegment.get(poolReader.readLong()).openReadStream());
			}else {
				for (int i = 0; i < poolEntries; i++) {
					pool.jump(i * POOL_ADDR_SIZE);
					K oneKey = keySerializer.read(keySegment.get(poolReader.readLong()).openReadStream());
					if (oneKey.equals(key))
						return valueSerializer.read(dataSegment.get(poolReader.readLong()).openReadStream());
				}
				return null;
			}
		}catch (IOException | ClassNotFoundException e){
			throw new RuntimeException(e);
		}
	}

	private Section getOrCreateHashPool(long keyHash) {
		Section s = getHashPool(keyHash);
		if(s == null)
			return createHashPool(keyHash);
		else
			return s;
	}
	private Section getHashPool(long keyHash) {
		try {
			DataInputStream dis = new DataInputStream(keyHashes.openReadStream());
			keyHashes.jump(0);
			for(int i = 0;i<getKeyPoolCount();i++){
				long hash = dis.readLong();
				long poolIndex = dis.readLong();
				if(hash == keyHash){
					return poolSegment.get(poolIndex);
				}
			}
			dis.close();
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Section createHashPool(long keyHash) {
		try {
			long cnt = getKeyPoolCount();
			DataOutputStream dos = new DataOutputStream(keyHashes.openWritingStream());
			keyHashes.jumpToEnd();
			dos.writeLong(keyHash);
			dos.writeLong(cnt);
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
		Set<K> keys = new HashSet<>();
		for (int i = 0; i < keySegment.sectionCount(); i++) {
			try {
				keys.add(keySerializer.read(keySegment.get(i).openReadStream()));
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		return keys;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return keySet().stream().map(k -> new SimpleEntry<K,V>(k,get(k) )).collect(Collectors.toSet());
	}

	@Override
	public V remove(Object key) {
		try{
		V val = get(key);
		Section hashPool = getHashPool(key.hashCode());
		DataInputStream poolReader = new DataInputStream(hashPool.openReadStream());
		long keyIndex = poolReader.readLong();
		long valIndex = poolReader.readLong();
		keySegment.deleteSegment(keyIndex);
		dataSegment.deleteSegment(valIndex);
		return val;
		}catch (IOException e){
			throw new RuntimeException(e);
		}
	}
}
