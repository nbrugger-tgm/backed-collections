package com.niton.memory.direct.managed;

import com.niton.memory.direct.DataStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

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
	private final DataStore dataStore;
	private final DataStore metaStore;
	private long blockSizePointer;
	private long endMarkPointer;
	private long startAddressPointer;
	private long endAddressPointer;
	private Section followUp = null;
	public byte shiftFlag = SHIFT_START_AND_END;
	private DataInputStream metaReader;
	private DataOutputStream metaWriter;
	private transient long startAddress,endAddress,endMark,blockSize;
	public Section(int configAddress, DataStore store,boolean read) {
		this(configAddress,store,store,BitSystem.X32,read);
	}
	public Section(int configAddress, DataStore store,DataStore metaStore,boolean read) {
		this(configAddress,store,metaStore,BitSystem.X32,read);
	}
	//blockSize,usedSize,startAddress,endAddress
	public Section(int configAddress, DataStore store,BitSystem system,boolean read) {
		this(configAddress,store,store,system,read);
	}
	public Section(int configAddress, DataStore store,DataStore metaStore,BitSystem system,boolean read) {
		this(
				store,
				metaStore,
				configAddress,
				configAddress+system.getBase(),
				configAddress+(system.getBase()*2),
				configAddress+(system.getBase()*3),
				system,
				read
		);
	}



	public Section(DataStore store, long blockSizePointer, long endMarkPointer, long startAddressPointer, long endAddressPointer,boolean read) {
		this(store,store,blockSizePointer,endMarkPointer,startAddressPointer,endAddressPointer,BitSystem.X32,read);
	}
	public Section(DataStore store,DataStore metaStore, long blockSizePointer, long endMarkPointer, long startAddressPointer, long endAddressPointer, BitSystem system,boolean read) {
		this.bit = system;
		this.dataStore = store;
		this.metaStore = metaStore;
		metaReader = new DataInputStream(metaStore.new DataStoreInputStream());
		metaWriter = new DataOutputStream(metaStore.new DataStoreOutputStream());
		this.blockSizePointer = blockSizePointer;
		this.endMarkPointer = endMarkPointer;
		this.startAddressPointer = startAddressPointer;
		this.endAddressPointer = endAddressPointer;
		if (read) {
			readMetadata();
		}
	}

	public long resolveAddress(long innerAddress){
		return innerAddress+getStartAddress();
	}
	public void readMetadata() {
		setBlockSizePointer(blockSizePointer);
		setEndMarkPointer(endMarkPointer);
		setStartAddressPointer(startAddressPointer);
		setEndAddressPointer(endAddressPointer);
	}

	public long getBlockSizePointer() {
		return blockSizePointer;
	}

	public void setBlockSizePointer(long blockSizePointer) {
		this.blockSizePointer = blockSizePointer;
		blockSize = readFromMetaData(blockSizePointer);
	}

	public long getEndMarkPointer() {
		return endMarkPointer;
	}

	public void setEndMarkPointer(long endMarkPointer) {
		this.endMarkPointer = endMarkPointer;
		endMark = readFromMetaData(endMarkPointer);
	}

	public long getStartAddressPointer() {
		return startAddressPointer;
	}

	public void setStartAddressPointer(long startAddressPointer) {
		this.startAddressPointer = startAddressPointer;
		startAddress = readFromMetaData(startAddressPointer);
	}

	public long getEndAddressPointer() {
		return endAddressPointer;
	}

	public void setEndAddressPointer(long endAddressPointer) {
		this.endAddressPointer = endAddressPointer;
		endAddress = readFromMetaData(endAddressPointer);
	}

	@Override
	public long size() {
		return endMark;
	}


	public void init(long blockSize, long initialBLocks, long startAddress){
		setStartAddress(startAddress);
		setEndMarker(0);
		setBlockSize(blockSize);
		setEndAddress(startAddress+(blockSize*initialBLocks));
		jump(0);
	}

	private void setStartAddress(long startAddress) {
		this.startAddress = startAddress;
		bit.write(startAddressPointer, startAddress,metaStore, metaWriter);
	}

	public long getBlockSize(){
		return blockSize;
	}
	public long getEndAddress(){
		return endAddress;
	}
	public long getStartAddress(){
		return startAddress;
	}

	private long readFromMetaData(long address) {
		metaStore.jump(address);
		try {
			switch (bit){
				case X8:
					return metaReader.readByte();
				case X16:
					return metaReader.readShort();
				case X32:
					return metaReader.readInt();
				case X64:
					return metaReader.readLong();
				default:
					throw new IllegalStateException("Unexpected value: " + bit);
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
		long start = getStartAddress();
		jump(to);
		return dataStore.read(start+from,start+to);
	}

	@Override
	protected void innerWrite(byte[] data, long from, long to) {
		while(to> capacity())
			addBlock();
		long strt= getStartAddress();
		dataStore.write(data,from+strt,to+strt);
		setEndMarker((int)Math.max(to, size()));
		jump(to);
	}


	/**
	 * Shifts the section itself towards index 0
	 */
	public void shiftBack(long amount) {
		long oldStart = getStartAddress();
		shiftNextSection(-amount);
		setStartAddress(getStartAddress()-amount);
		setEndAddress(getEndAddress()-amount);
		dataStore.jump(oldStart);
		dataStore.shiftAll(-amount);
	}
	public void shiftForward(long amount) {
		long oldStart = getStartAddress();
		shiftNextSection(amount);
		setStartAddress(getStartAddress()+amount);
		setEndAddress(getEndAddress()+amount);
		dataStore.jump(oldStart);
		dataStore.shiftAll(amount);
	}
	public void addBlock() {
		long origEnd = getEndAddress();
		long blcSz = getBlockSize();
		dataStore.jump(origEnd);
		dataStore.shiftAll(blcSz);
		setEndAddress(origEnd+blcSz);
		shiftNextSection(blcSz);
	}
	private void shiftNextSection(long blcSz){
		NextShifter shifter = new NextShifter();
		while (shifter != null){
			shifter = shifter.shiftNextSection(blcSz);
		}
	}



	private class NextShifter {
		public NextShifter shiftNextSection(long blcSz){
			if(followUp != null){
				followUp.setEndAddress(followUp.getEndAddress()+blcSz);
				if(shiftFlag == SHIFT_START_AND_END || followUp.followUp == null){
					followUp.setStartAddress(followUp.getStartAddress()+blcSz);
				}
				NextShifter followShifter = followUp.new NextShifter();
				followUp.refreshCaches();
				return followShifter;
			}
			return null;
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
		setEndMarker(from);
		jump(from);
		return  ret;
	}

	public long capacity() {
		return getEndAddress()- getStartAddress();
	}
	public void setEndAddress(long endAddress) {
		bit.write(endAddressPointer, endAddress,metaStore, metaWriter);
		this.endAddress = endAddress;
	}

	/**
	 * Shifts the header to a different location
	 * @param offset the ammount to shift (negative values shift towards index 0)
	 * @param shiftData if true also the data is shifted if false only the addresses change
	 */
	public void shiftHeader(int offset, boolean shiftData) {
		if(shiftData){
			metaStore.jump(getBlockSizePointer());
			metaStore.shift(offset,bit.getBase()*4);
		}
		setEndAddressPointer(getEndAddressPointer()+offset);
		setStartAddressPointer(getStartAddressPointer()+offset);
		setBlockSizePointer(getBlockSizePointer()+offset);
		setEndMarkPointer(getEndMarkPointer()+offset);

	}

	public void enlarge(long mvSize) {
		long origEnd = getEndAddress();
		dataStore.jump(origEnd);
		dataStore.shiftAll(mvSize);
		setEndAddress(origEnd+mvSize);
		shiftNextSection(mvSize);
	}

	public void setEndMarker(long endMarker) {
		bit.write(endMarkPointer, endMarker,metaStore, metaWriter);
		this.endMark = endMarker;
	}

	public void setBlockSize(long i) {
		bit.write(blockSizePointer, i,metaStore, metaWriter);
		this.blockSize = i;
	}

	public void refreshCaches() {
		setEndAddressPointer(endAddressPointer);
		setStartAddressPointer(startAddressPointer);
		setEndMarkPointer(endMarkPointer);
		setBlockSizePointer(blockSizePointer);
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
		return "(startAddress:"+ getStartAddress()+", blockSize:"+ getBlockSize()+", usedSize:"+ size()+", endAddress:"+getEndAddress()+")"+super.toString();

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

		return Objects.equals(followUp, section.followUp);
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
