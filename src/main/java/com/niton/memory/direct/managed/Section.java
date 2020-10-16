package com.niton.memory.direct.managed;

import com.niton.memory.direct.DataStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Section extends DataStore {
	public BitSystem getBit() {
		return bit;
	}

	private final BitSystem bit;
	/**
	 * Shifts start and end of the next Section when shifted
	 */
	public static final byte SHIFT_START_AND_END = 1;
	/**
	 * Only Shifts the end of the next Section when shifted itself
	 */
	public static final byte SHIFT_END = 2;

	public int getHeaderSize() {
		return  bit.getBase()/*start*/ +
				bit.getBase()/*end*/ +
				bit.getBase()/*size*/ +
				bit.getBase()/*block-size*/;
	}
	private final DataStore store;
	private long blockSizePointer;
	private long endMarkPointer;
	private long startAddressPointer;
	private long endAddressPointer;
	private Section followUp = null;
	public byte shiftFlag = SHIFT_START_AND_END;
	private DataInputStream dis;
	private DataOutputStream dos;
	//blockSize,usedSize,startAddress,endAddress
	public Section(int configAddress, DataStore store,BitSystem system) {
		this(store,configAddress,configAddress+=system.getBase(),configAddress+=system.getBase(),configAddress+system.getBase(),system);
	}
	public Section(int configAddress, DataStore store) {
		this(configAddress,store,BitSystem.x32);
	}
	public long resolveAddress(long innerAddress){
		return innerAddress+getStartAddress();
	}
	public Section(DataStore store, long blockSizePointer, long endMarkPointer, long startAddressPointer, long endAddressPointer) {
		this(store,blockSizePointer,endMarkPointer,startAddressPointer,endAddressPointer,BitSystem.x32);
	}
	public Section(DataStore store, long blockSizePointer, long endMarkPointer, long startAddressPointer, long endAddressPointer, BitSystem system) {
		this.bit = system;
		this.store = store;
		this.blockSizePointer = blockSizePointer;
		this.endMarkPointer = endMarkPointer;
		this.startAddressPointer = startAddressPointer;
		this.endAddressPointer = endAddressPointer;
		dis = new DataInputStream(store.new DataStoreInputStream());
		dos = new DataOutputStream(store.new DataStoreOutputStream());
	}

	public long getBlockSizePointer() {
		return blockSizePointer;
	}

	public void setBlockSizePointer(long blockSizePointer) {
		this.blockSizePointer = blockSizePointer;
	}

	public long getEndMarkPointer() {
		return endMarkPointer;
	}

	public void setEndMarkPointer(long endMarkPointer) {
		this.endMarkPointer = endMarkPointer;
	}

	public long getStartAddressPointer() {
		return startAddressPointer;
	}

	public void setStartAddressPointer(long startAddressPointer) {
		this.startAddressPointer = startAddressPointer;
	}

	public long getEndAddressPointer() {
		return endAddressPointer;
	}

	public void setEndAddressPointer(long endAddressPointer) {
		this.endAddressPointer = endAddressPointer;
	}

	@Override
	public long size() {
		return readFromAddress(endMarkPointer);
	}


	public void init(long blockSize, long initialBLocks, long startAddress){
		writeToAddress(startAddressPointer, startAddress);
		writeToAddress(endMarkPointer, 0);
		writeToAddress(blockSizePointer, blockSize);
		writeToAddress(endAddressPointer, startAddress+(blockSize*initialBLocks));
	}
	public long getBlockSize(){
		return readFromAddress(blockSizePointer);
	}
	public long getEndAddress(){
		return readFromAddress(endAddressPointer);
	}
	public long getStartAddress(){
		return readFromAddress(startAddressPointer);
	}

	private long readFromAddress(long address) {
		store.jump(address);
		try {
			switch (bit){
				case x8:
					return dis.readByte();
				case x16:
					return dis.readShort();
				case x32:
					return dis.readInt();
				case x64:
					return dis.readLong();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	protected byte[] innerRead(long from, long to) {
		if(from < 0 || to > getEndAddress())
			throw new SegmentationFault("Read outside the section (Sect: "+getStartAddress()+" - "+getEndAddress()+") READ: "+from+" - "+to);
		long start = readFromAddress(startAddressPointer);
		jump(to);
		return store.read(start+from,start+to);
	}

	@Override
	protected void innerWrite(byte[] data, long from, long to) {
		while(to> capacity())
			addBlock();
		long strt= readFromAddress(startAddressPointer);
		store.write(data,from+strt,to+strt);
		writeToAddress(endMarkPointer,(int)Math.max(to, readFromAddress(endMarkPointer)));
		jump(to);
	}

	private void writeToAddress(long address, long i) {
		store.jump(address);
		try {
			switch (bit){
				case x8:
					dos.writeByte((int) i);
					break;
				case x16:
					dos.writeShort((int) i);
					break;
				case x32:
					dos.writeInt((int) i);
					break;
				case x64:
					dos.writeLong(i);
					break;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Shifts the section itself towards index 0
	 */
	public void shiftBack(long amount) {
		long oldStart = getStartAddress();
		shiftNextSection(-amount);
		writeToAddress(startAddressPointer,getStartAddress()-amount);
		writeToAddress(endAddressPointer,getEndAddress()-amount);
		store.jump(oldStart);
		store.shiftAll(-amount);
	}
	public void shiftForward(long amount) {
		long oldStart = getStartAddress();
		shiftNextSection(amount);
		writeToAddress(startAddressPointer,getStartAddress()+amount);
		writeToAddress(endAddressPointer,getEndAddress()+amount);
		store.jump(oldStart);
		store.shiftAll(amount);
	}
	public void addBlock() {
		long origEnd = getEndAddress();
		long blcSz = getBlockSize();
		store.jump(origEnd);
		store.shiftAll(blcSz);
		writeToAddress(endAddressPointer,origEnd+blcSz);
		shiftNextSection(blcSz);
	}
	private void shiftNextSection(long blcSz){
		if(followUp != null){
			followUp.writeToAddress(followUp.endAddressPointer, followUp.getEndAddress()+blcSz);
			if(shiftFlag == SHIFT_START_AND_END){
				followUp.writeToAddress(followUp.startAddressPointer, followUp.getStartAddress()+blcSz);
			}
			followUp.shiftNextSection(blcSz);
		}
	}
	public void enableRefShifting(Section sect) {
		this.followUp = sect;
	}

	@Override
	public long cut(long from) {
		if(from > size())
			throw new IllegalArgumentException("You cannot cut outside the data size (cut-point:"+from+", actual size:"+size());
		long ret = capacity()-from;
		writeToAddress(endMarkPointer, (int) from);
		jump(from);
		return  ret;
	}

	public long capacity() {
		return readFromAddress(endAddressPointer)- readFromAddress(startAddressPointer);
	}

	public void setEndAddress(long oldIndexEnd) {
		writeToAddress(endAddressPointer,oldIndexEnd);
	}

	/**
	 * Shifts the hader to a different location
	 * @param offset the ammount to shift (negative values shift towards index 0)
	 * @param shiftData if true also the data is shifted if false only the addresses change
	 */
	public void shiftHeader(int offset, boolean shiftData) {
		if(shiftData){
			store.jump(getBlockSizePointer());
			store.shift(offset,bit.getBase()*4);
		}
		setEndAddressPointer(getEndAddressPointer()+offset);
		setStartAddressPointer(getStartAddressPointer()+offset);
		setBlockSizePointer(getBlockSizePointer()+offset);
		setEndMarkPointer(getEndMarkPointer()+offset);

	}

	public void enlarge(long mvSize) {
		long origEnd = getEndAddress();
		store.jump(origEnd);
		store.shiftAll(mvSize);
		writeToAddress(endAddressPointer,origEnd+mvSize);
		shiftNextSection(mvSize);
	}

	public void setEndMarker(long oldSize) {
		writeToAddress(endMarkPointer,oldSize);
	}

	public static class SegmentationFault extends RuntimeException {
		public SegmentationFault(String message) {
			super(message);
		}
	}

	@Override
	public String toString() {
		return printInfoAndData();
	}
	public String printInfoAndData(){
		return "(startAddress:"+ readFromAddress(startAddressPointer)+", blockSize:"+ readFromAddress(blockSizePointer)+", usedSize:"+ readFromAddress(endMarkPointer)+", endAddress:"+ readFromAddress(endAddressPointer)+")"+super.toString();

	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Section section = (Section) o;

		if (blockSizePointer != section.blockSizePointer) return false;
		if (endMarkPointer != section.endMarkPointer) return false;
		if (startAddressPointer != section.startAddressPointer) return false;
		if (endAddressPointer != section.endAddressPointer) return false;
		if (shiftFlag != section.shiftFlag) return false;
		return followUp != null ? followUp.equals(section.followUp) : section.followUp == null;
	}

	@Override
	public int hashCode() {
		int result = (int) (blockSizePointer ^ (blockSizePointer >>> 32));
		result = 31 * result + (int) (endMarkPointer ^ (endMarkPointer >>> 32));
		result = 31 * result + (int) (startAddressPointer ^ (startAddressPointer >>> 32));
		result = 31 * result + (int) (endAddressPointer ^ (endAddressPointer >>> 32));
		result = 31 * result + (followUp != null ? followUp.hashCode() : 0);
		result = 31 * result + (int) shiftFlag;
		return result;
	}
}
