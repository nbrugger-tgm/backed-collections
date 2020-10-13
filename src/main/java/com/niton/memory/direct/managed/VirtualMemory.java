package com.niton.memory.direct.managed;

import com.niton.memory.direct.stores.DataStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;

public class VirtualMemory {
	private final DataStore data;
	private Section index;
	private DataOutputStream indexDos,mainDos;
	private DataInputStream indexDis,mainDis;
	public static final int SECTION_HEADER_SIZE = Section.HEADER_SIZE-8;
	private final ArrayList<Section> sectionCache = new ArrayList<>();

	public VirtualMemory(DataStore data) {
		this.data = data;
		index = new Section(0, data);
		indexDos = new DataOutputStream(index.new DataStoreOutputStream());
		indexDis = new DataInputStream(index.new DataStoreInputStream());
		mainDos = new DataOutputStream(data.new DataStoreOutputStream());
		mainDis = new DataInputStream(data.new DataStoreInputStream());
	}
	public void initIndex(int enlargementInterval){
		if(enlargementInterval < 1)
			throw new IllegalArgumentException("Enlargement intervall must be > 0");
		index.init(Section.HEADER_SIZE*enlargementInterval,1,Section.HEADER_SIZE);
	}

	public long readSize() {
		return index.size()/SECTION_HEADER_SIZE;
	}
	public void readIndex(){
		for (int i = 0; i < readSize(); i++) {
			readSection(i);
		}
	}
	protected Section readSection(long i){
		Section sect = new Section(this.data,
				index.resolveAddress(i*SECTION_HEADER_SIZE),//block
				index.resolveAddress(i*SECTION_HEADER_SIZE+8),//size
				index.resolveAddress(i*SECTION_HEADER_SIZE-8),//start
				index.resolveAddress(i*SECTION_HEADER_SIZE+16));//end
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
		try {
			long priorSize = readSize();
			Section sect = readSection(priorSize);
			index.jump(priorSize*SECTION_HEADER_SIZE);
			indexDos.writeLong(blockSize);//blocksize
			indexDos.writeLong(0);     //currentSize
			indexDos.writeLong(sect.getStartAddress()+blockSize*initialBlocks); //end
			return sect;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	public Section get(long i){
		if(i<0)
			throw new IllegalArgumentException("you cant get negative indices");
		return sectionCache.get((int) i);
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("VirtualMemory{\n");
		long s = readSize();
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
	private static String humanReadableByteCountSI(long bytes) {
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
}
