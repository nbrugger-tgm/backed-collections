package com.niton.collections.backed.managed;

import com.niton.collections.backed.stores.DataStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Section extends DataStore {
	public static final byte SHIFT_START_AND_END = 1;
	public static final byte SHIFT_END = 2;
	public static final int HEADER_SIZE =
			8/*start*/+
			8/*end*/+
			8/*size*/+
			8/*block-size*/;
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
	public Section(int configAddress, DataStore store) {
		this(store,configAddress,configAddress+=8,configAddress+=8,configAddress+8);
	}
	public long resolveAddress(long innerAddress){
		return innerAddress+getStartAddress();
	}

	public Section(DataStore store, long blockSizePointer, long endMarkPointer, long startAddressPointer, long endAddressPointer) {
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
			return dis.readLong();
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
		if(to>= capacity())
			addBlock();
		long strt= readFromAddress(startAddressPointer);
		store.write(data,from+strt,to+strt);
		writeToAddress(endMarkPointer,(int)Math.max(to, readFromAddress(endMarkPointer)));
		jump(to);
	}

	private void writeToAddress(long address, long i) {
		store.jump(address);
		try {
			dos.writeLong(i);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public void addBlock() {
		long origEnd = getEndAddress();
		long blcSz = getBlockSize();
		store.jump(origEnd);
		store.shiftAll(blcSz);
		writeToAddress(endAddressPointer,origEnd+blcSz);
		shiftSection(blcSz);
	}
	private void shiftSection(long blcSz){
		if(followUp != null){
			followUp.writeToAddress(followUp.endAddressPointer, followUp.getEndAddress()+blcSz);
			if(shiftFlag == SHIFT_START_AND_END){
				followUp.writeToAddress(followUp.startAddressPointer, followUp.getStartAddress()+blcSz);
			}
			followUp.shiftSection(blcSz);
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

	public static class SegmentationFault extends RuntimeException {
		public SegmentationFault(String message) {
			super(message);
		}
	}

	@Override
	public String toString() {
		return "(startAddress:"+ readFromAddress(startAddressPointer)+", blockSize:"+ readFromAddress(blockSizePointer)+", usedSize:"+ readFromAddress(endMarkPointer)+", endAddress:"+ readFromAddress(endAddressPointer)+")"+super.toString();
	}
}
