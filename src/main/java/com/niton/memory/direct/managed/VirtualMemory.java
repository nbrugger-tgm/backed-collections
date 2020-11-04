package com.niton.memory.direct.managed;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;

import com.niton.StorageException;
import com.niton.memory.direct.DataStore;

public class VirtualMemory {
	private final DataStore data;
	private Section index;
	private DataOutputStream indexDos,mainDos;
	private DataInputStream indexDis,mainDis;
	private BitSystem bits;
	public int getSectionHeaderSize(){
		return 3*bits.getBase();
	}
	private final ArrayList<Section> sectionCache = new ArrayList<>();

	public VirtualMemory(DataStore data) {
		this(data,BitSystem.X32);
	}
	public VirtualMemory(DataStore data,BitSystem bits) {
		this.data = data;
		this.bits = bits;
		mainDos = new DataOutputStream(data.new DataStoreOutputStream());
		mainDis = new DataInputStream(data.new DataStoreInputStream());
	}
	private class IndexSection extends Section {
		public IndexSection(int configAddress, DataStore store, BitSystem system) {
			super(configAddress, store, system);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Index: (size:").append(size()).append(", end:").append(getEndAddress()).append(")\n");
			DataInputStream dis = new DataInputStream(this.openReadStream());
			long old = this.getMarker();
			this.jump(0);
			long sz = size()/getSectionHeaderSize();
			for (int i = 0; i < sz; i++) {
				printIndexEntry(builder, dis);
			}
			this.jump(old);
			return builder.toString();
		}
	}

	private void printIndexEntry(StringBuilder builder, DataInputStream dis){
		try {
			long blk,sz,end;
			switch (bits){
				case X8:
					blk = dis.readByte();
					sz= dis.readByte();
					end = dis.readByte();
					break;
				case X16:
					blk = dis.readShort();
					sz= dis.readShort();
					end = dis.readShort();
					break;
				case X32:
					blk = dis.readInt();
					sz= dis.readInt();
					end = dis.readInt();
					break;
				case X64:
					blk = dis.readLong();
					sz= dis.readLong();
					end = dis.readLong();
					break;
				default:
					throw new IllegalStateException("Unexpected value: " + bits);
			}
			builder
				.append("[blck:")
				.append(blk)
				.append(", size:")
				.append(sz)
				.append(", end:")
				.append(end)
				.append("]\n");
		}catch (Exception e){
			throw new StorageException(e);
		}
	}

	public void initIndex(int enlargementInterval){
		if(enlargementInterval < 1)
			throw new IllegalArgumentException("Enlargement intervall must be > 0");

		sectionCache.clear();
		data.jump(0);

		bits.write(data.getMarker(),0,data,mainDos);
		bits.write(data.getMarker(),0,data,mainDos);
		bits.write(data.getMarker(),0,data,mainDos);
		bits.write(data.getMarker(),0,data,mainDos);

		index = new IndexSection(0, data,bits);
		index.refreshCaches();
		indexDos = new DataOutputStream(index.new DataStoreOutputStream());
		indexDis = new DataInputStream(index.new DataStoreInputStream());

		index.init(getSectionHeaderSize()*enlargementInterval,1,index.getHeaderSize());
	}
	public void readIndex(){
		sectionCache.clear();
		index = new IndexSection(0, data,bits);
		index.refreshCaches();
		indexDos = new DataOutputStream(index.new DataStoreOutputStream());
		indexDis = new DataInputStream(index.new DataStoreInputStream());
		for (int i = 0; i < sectionCount(); i++) {
			readSection(i);
		}
	}
	public Section getIndex() {
		return index;
	}


	public long sectionCount() {
		return index.size()/getSectionHeaderSize();
	}
	protected Section readSection(long i){
		Section sect = new Section(this.data,
				index.resolveAddress(i*getSectionHeaderSize()),//block
				index.resolveAddress(i*getSectionHeaderSize()+getBits().getBase()),//size
				index.resolveAddress(i*getSectionHeaderSize()-getBits().getBase()),//start
				index.resolveAddress(i*getSectionHeaderSize()+(getBits().getBase()*2))
				,bits);//end
		if(i == 0){
			sect.setStartAddressPointer(index.getEndAddressPointer());
			index.enableRefShifting(sect);
			index.shiftFlag = Section.SHIFT_END;
		}else{
			Section previous = get(i-1);
			previous.enableRefShifting(sect);
			previous.shiftFlag = Section.SHIFT_END;
		}
		sectionCache.add(sect);
		return sect;
	}
	public Section createSection(long blockSize, long initialBlocks){
		long priorSize = sectionCount();
		Section sect = readSection(priorSize);
		index.jump(priorSize*getSectionHeaderSize());

		bits.write(index.getMarker(),blockSize,index,indexDos);
		bits.write(index.getMarker(),0,index,indexDos);
		bits.write(index.getMarker(),sect.getStartAddress()+blockSize*initialBlocks,index,indexDos);
		sect.refreshCaches();
		return sect;
	}


	public Section get(long i){
		if(i<0)
			throw new IllegalArgumentException("you cant get negative indices");
		return sectionCache.get((int) i);
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("VirtualMemory{\n");
		long s = sectionCount();
		for (long i = 0; i < s; i++) {
			Section sect = get(i);
			sb.append("    sect[");
			sb.append(humanReadableByteCountSI(sect.size()));
			sb.append("/");
			sb.append(humanReadableByteCountSI(sect.capacity()));
			sb.append("]\n");
		}
		sb.append('}');
		return sb.toString();
	}
	public static String humanReadableByteCountSI(long bytes) {
		if (-1000 < bytes && bytes < 1000) {
			return bytes + " B";
		}
		CharacterIterator ci = new StringCharacterIterator("kMGTPE");
		while (bytes <= -999_950 || bytes >= 999_950) {
			bytes /= 1000;
			ci.next();
		}
		return String.format("%.1f %cB", bytes / 1000.0, ci.current());
	}

	public Section createOrGetSection(long blockSize, long initialBlocks, long index) {
		if(sectionCount() > index)
			return get(index);
		else
			return createSection(blockSize,initialBlocks);
	}

	public void deleteSegment(long index) {
		if(index >= sectionCount() || index < 0)
			throw new IndexOutOfBoundsException((int) index);
		if(sectionCount() == index+1)
			this.index.cut(index*getSectionHeaderSize());
		else
			shiftSegmentsBackwards(index+1);
		sectionCache.remove(index);
	}

	/**
	 * @param from the index of the section from which on should be shifted backwards inclusive
	 */
	private void shiftSegmentsBackwards(long from) {
		long mvSize = get(from-1).capacity();
		get(from).shiftBack(mvSize);
		this.index.jump(from*getSectionHeaderSize());
		index.shiftAll(-getSectionHeaderSize());
		index.cut(index.size()-getSectionHeaderSize());
		readIndex();
		data.cut(get(sectionCount()-1).getEndAddress());
	}

	public Section insertSection(int i,long blockSize,long initialBlocks) {
		if(i == sectionCount()) {
			return createSection(blockSize,initialBlocks);
		}
		long mvSize = blockSize*initialBlocks;

		Section old = get(i);
		long oldSize = old.size();
		old.enlarge(mvSize);
		old.jump(0);
		old.shiftAll(mvSize);

		if(index.capacity()-index.size() == 0)
			index.enlarge(getSectionHeaderSize());
		long oldStart = old.getStartAddress();
		index.jump(i*getSectionHeaderSize());
		index.shiftAll(getSectionHeaderSize());

		old.shiftHeader(getSectionHeaderSize(),false);
		old.setEndMarker(oldSize);
		Section s = new Section(0,this.data,bits);
		long indexStart = index.getStartAddress();
		s.setStartAddressPointer(indexStart+i*getSectionHeaderSize()-bits.getBase());
		s.setBlockSizePointer(indexStart+i*getSectionHeaderSize());
		s.setEndMarkPointer(indexStart+i*getSectionHeaderSize()+bits.getBase());
		s.setEndAddressPointer(indexStart+i*getSectionHeaderSize()+ (bits.getBase()*2));
		s.init(blockSize,initialBlocks,oldStart);
		readIndex();
		return get(i);
	}
	public String printIndex(){
		try {
			StringBuilder s = new StringBuilder();
			s.append(index).append("\n");

			long sections = sectionCount();
			index.jump(0);

			for (int i = 0; i < sections; i++) {
				if(i==0)
					s.append("[   0   ]");
				else{
					index.skip(getSectionHeaderSize());
					s.append("[   ").append(indexDis.readLong()).append("   ]");
				}
			}

			return s.toString();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	public BitSystem getBits() {
		return bits;
	}


	public void setIndexIncrement(int i) {
		getIndex().setBlockSize(i*getSectionHeaderSize());
	}
}
