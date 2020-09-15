package com.niton.collections.backed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class DataStore {
	public int bufferSize = 1024*8;
	private int marker = 0;
	public abstract int size();
	public static final int byteShift = Byte.MIN_VALUE;
	public int read() {
		return unsignedByte(innerRead(marker, marker +1))[0];
	}

	private int[] unsignedByte(byte[] innerRead) {
		int[] unsigned = new int[innerRead.length];
		for (int i = 0; i < innerRead.length; i++) {
			unsigned[i] = innerRead[i]-byteShift;
		}
		return unsigned;
	}

	private byte[] signedByte(int[] innerRead) {
		byte[] unsigned = new byte[innerRead.length];
		for (int i = 0; i < innerRead.length; i++) {
			unsigned[i] = (byte) (innerRead[i]+byteShift);
		}
		return unsigned;
	}

	public byte[] readNext(int len) {
		return innerRead(marker, marker +len);
	}

	public byte[] read(int from, int to) {
		return innerRead(from,to);
	}

	/**
	 * Reads a portion of the datastore
	 * @param from the index (inclusive) in the datastore to start the read from
	 * @param to the index (exclusive) in the datastore to start in the datastore to end the read at
	 * @return the data
	 */
	protected abstract byte[] innerRead(int from,int to);

	public int read(int index) {
		return unsignedByte(innerRead(index,index++))[0];
	}

	public void write(int data) {
		innerWrite(signedByte(new int[]{data}), marker, marker +1);
	}

	public void writeNext(byte[] data, int len) {
		innerWrite(data, marker, marker +len);
	}
	public void write(byte[] data){
		innerWrite(data, marker, marker + data.length);
	}
	public void write(byte[] data, int from, int to) {
		innerWrite(data,from,to);
	}

	/**
	 * Writes data into the datatastore
	 * @param data the data to write
	 * @param from the index to start writing to
	 * @param to the last index to write to
	 * @return
	 */
	protected abstract void innerWrite(byte[] data,int from,int to);

	public int skip(int size){
		marker += size;
		return marker;
	}
	public int rewind(int size){
		marker -= size;
		return marker;
	}
	public int jumpToEnd(){
		marker = size();
		return marker;
	}

	public int getMarker() {
		return marker;
	}

	/**
	 * alias for set pointer
	 * @param index
	 */
	public void jump(int index){
		marker = index;
	}

	public void setMarker(int marker) {
		this.marker = marker;
	}

	public void shiftAll(int offset) {
		shift(offset, size()- marker);
	}

	public class DataStoreOutputStream extends OutputStream {
		@Override
		public void write(int b) throws IOException {
			DataStore.this.write(b);
		}
	}

	public class DataStoreInputStream extends InputStream {
		@Override
		public int read() throws IOException {
			return DataStore.this.read();
		}
	}
	public class ShiftingOutputStream extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			shiftWrite(b);
		}
	}

	/**
	 * Writes the byte shifting all bytes (including the old byte) by +1
	 * @param b
	 */
	public void shiftWrite(int b){
		int origin = marker;
		if(origin<size()) {
			shiftAll(1);
			jump(origin);
		}
		write(b);
	}
	public void shiftWrite(byte[] data){
		int origin = getMarker();
		shiftAll(data.length);
		jump(origin);
		write(data);
	}


	/**
	 * Removes all bytes after from (exclusive)
	 * @param from the index to remove bytes (exclusive)
	 * @return the number of bytes removed
	 */
	public abstract int cut(int from);
	public int cut(){
		return cut(marker);
	}

	/**
	 * Shifts the pointed byte by a certain ammount.
	 * The original position of the byte stays unchanged
	 * @param offset the numbers of bytes to shift the byte (negative value shifts it backwards/towards file start)
	 */
	public void shift(int offset){
		int val = read();
		skip(offset-1);
		write(val);
	}

	/**
	 * Shifts a portion of the array around
	 * @param offset the ammount to shift the array
	 * @param lenght the ammount of bytes to shift
	 * @see #shift(int)
	 */
	public void shift(int offset,int lenght){
		if(getMarker()>size())
			return;
		boolean startAtEnd = offset>0;
		int origin = getMarker();
		if(startAtEnd) {
			while (lenght > bufferSize) {
				jump(origin+(lenght -= bufferSize));
				shift(offset, bufferSize);
			}
		}else{
			while (lenght > bufferSize){
				shift(offset,bufferSize);
				skip(offset);
				lenght-=bufferSize;
			}
		}
		jump(origin);
		byte[] dataToShift = readNext(lenght);
		jump(origin+offset);
		write(dataToShift);
	}
}
