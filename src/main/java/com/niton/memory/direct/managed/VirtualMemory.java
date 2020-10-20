package com.niton.memory.direct.managed;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.BitSet;

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
		this(data,BitSystem.x32);
	}
	public VirtualMemory(DataStore data,BitSystem bits) {
		this.data = data;
		this.bits = bits;
		index = new Section(0, data,bits){
			@Override
			public String toString() {
				StringBuilder builder = new StringBuilder();
				builder.append("Index: (size:"+size()+", end:"+getEndAddress()+"\n");
				DataInputStream dis = new DataInputStream(this.openReadStream());
				long old = this.getMarker();
				this.jump(0);
				long sz = size()/getSectionHeaderSize();
				for (int i = 0; i < sz; i++) {
					try {
						switch (bits){

							case x8:
								builder.append("[blck:"+dis.readByte()+", size:"+dis.readByte()+", end:"+dis.readByte()+"]\n");
								break;
							case x16:
								builder.append("[blck:"+dis.readShort()+", size:"+dis.readShort()+", end:"+dis.readShort()+"]\n");
								break;
							case x32:
								builder.append("[blck:"+dis.readInt()+", size:"+dis.readInt()+", end:"+dis.readInt()+"]\n");
								break;
							case x64:
								builder.append("[blck:"+dis.readLong()+", size:"+dis.readLong()+", end:"+dis.readLong()+"]\n");
								break;
						}

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				return builder.toString();
			}
		};
		indexDos = new DataOutputStream(index.new DataStoreOutputStream());
		indexDis = new DataInputStream(index.new DataStoreInputStream());
		mainDos = new DataOutputStream(data.new DataStoreOutputStream());
		mainDis = new DataInputStream(data.new DataStoreInputStream());
	}

	public Section getIndex() {
		return index;
	}

	public void initIndex(int enlargementInterval){
		sectionCache.clear();
		if(enlargementInterval < 1)
			throw new IllegalArgumentException("Enlargement intervall must be > 0");
		index.init(getSectionHeaderSize()*enlargementInterval,1,index.getHeaderSize());
	}

	public long sectionCount() {
		return index.size()/getSectionHeaderSize();
	}
	public void readIndex(){
		sectionCache.clear();
		for (int i = 0; i < sectionCount(); i++) {
			readSection(i);
		}
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

		writeNumber(indexDos,blockSize);
		writeNumber(indexDos,0);     //currentSize
		writeNumber(indexDos,sect.getStartAddress()+blockSize*initialBlocks); //end
		return sect;
	}

	private void writeNumber(DataOutputStream indexDos, long blockSize) {
		try {
			switch (bits) {
				case x8:
					indexDos.writeByte((byte) blockSize);
					break;
				case x16:
					indexDos.writeShort((short) blockSize);
					break;
				case x32:
					indexDos.writeInt((int) blockSize);
					break;
				case x64:
						indexDos.writeLong(blockSize);
					break;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		String s = "";
		s+=index+"\n";
		long sections = sectionCount();
		index.jump(0);
		for (int i = 0; i < sections; i++) {
			if(i==0)
				s += "[   0   ]";
			else{
				index.skip(getSectionHeaderSize());
				try {
					s+=  "[   "+indexDis.readLong()+"   ]";
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		return s;
	}

	public BitSystem getBits() {
		return bits;
	}


	public void setIndexIncrement(int i) {
		getIndex().setBlockSize(i*getSectionHeaderSize());
	}
}
