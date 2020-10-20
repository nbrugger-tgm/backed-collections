package com.niton.memory.direct;

import com.niton.collections.backed.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.*;

public abstract class DataStore {
	public int bufferSize = 1024*4;
	private long marker = 0;

	/**
	 * @return the number of actual used bytes. NOT THE MAXIMUM SIZE
	 */
	public abstract long size();
	public static final int byteShift = Byte.MIN_VALUE;
	public int read() {
		return unsignedByte(innerRead(marker, marker +1))[0];
	}

	public static int[] unsignedByte(byte[] innerRead) {
		int[] unsigned = new int[innerRead.length];
		for (int i = 0; i < innerRead.length; i++) {
			unsigned[i] = innerRead[i]& 0xFF;
		}
		return unsigned;
	}

	public static byte[] signedByte(int[] innerRead) {
		byte[] unsigned = new byte[innerRead.length];
		for (int i = 0; i < innerRead.length; i++) {
			unsigned[i] = (byte) innerRead[i];
		}
		return unsigned;
	}

	public byte[] readNext(long len) {
		return innerRead(marker, marker +len);
	}

	/**
	 *
	 * @param from inlclusive
	 * @param to exclusive
	 * @return the array of data
	 */
	public byte[] read(long from, long to) {
		return innerRead(from,to);
	}

	/**
	 * Reads a portion of the datastore
	 * @param from the index (inclusive) in the datastore to start the read from
	 * @param to the index (exclusive) in the datastore to start in the datastore to end the read at
	 * @return the data
	 */
	protected abstract byte[] innerRead(long from,long to);

	public int read(long index) {
		return unsignedByte(innerRead(index,++index))[0];
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
	public void write(byte[] data, long from, long to) {
		innerWrite(data,from,to);
	}

	/**
	 * Writes data into the datatastore
	 * @param data the data to write
	 * @param from the index to start writing to
	 * @param to the last index to write to
	 * @return
	 */
	protected abstract void innerWrite(byte[] data,long from,long to);

	public long skip(long size){
		marker += size;
		return marker;
	}
	public long rewind(long size){
		marker -= size;
		return marker;
	}
	public long jumpToEnd(){
		marker = size();
		return marker;
	}

	public long getMarker() {
		return marker;
	}

	/**
	 * alias for set pointer
	 * @param index
	 */
	public void jump(long index){
		marker = index;
	}

	public void setMarker(long marker) {
		this.marker = marker;
	}

	public void shiftAll(long offset) {
		shift(offset, size()- marker);
	}

	/**
	 * @param keySerializer
	 * @param <K>
	 * @throws RuntimeException when reading goes wrong
	 * @return
	 */
	public <K> K read(Serializer<K> keySerializer) {
		jump(0);
		try {
			return keySerializer.read(openReadStream());
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> void write(T value, Serializer<T> valueSerializer) {
		jump(0);
		try {
			valueSerializer.write(value,openWritingStream());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	public class DataStoreOutputStream extends OutputStream {
		@Override
		public void write(int b) throws IOException {
			DataStore.this.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			innerWrite(Arrays.copyOfRange(b,off,off+len),marker,marker+len);
		}
	}

	public class DataStoreInputStream extends InputStream {
		@Override
		public int read() throws IOException {
			int r =  DataStore.this.read();
			return r;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			System.arraycopy(DataStore.this.innerRead(marker,marker+len),0,b,off,len);
			return len;
		}
	}
	public class ShiftingOutputStream extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			shiftWrite(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			shiftWrite(Arrays.copyOfRange(b, off, off+len));
		}
	}

	/**
	 * Writes the byte shifting all bytes (including the old byte) by +1
	 * @param b
	 */
	public void shiftWrite(int b){
		long origin = marker;
		if(origin<size()) {
			shiftAll(1);
			jump(origin);
		}
		write(b);
	}
	public void shiftWrite(byte[] data){
		long origin = getMarker();
		shiftAll(data.length);
		jump(origin);
		write(data);
	}


	/**
	 * Removes all bytes after from (exclusive)
	 * @param from the index to remove bytes (exclusive)
	 * @return the number of bytes removed
	 */
	public abstract long cut(long from);
	public long cut(){
		return cut(marker);
	}

	/**
	 * Shifts the pointed byte by a certain ammount.
	 * The original position of the byte stays unchanged
	 * @param offset the numbers of bytes to shift the byte (negative value shifts it backwards/towards file start)
	 */
	public void shift(long offset){
		int val = read();
		skip(offset-1);
		write(val);
	}

	/**
	 * Shifts a portion of the array around
	 * @param offset the ammount to shift the array (negative value shifts it backwards/towards index 0)
	 * @param lenght the ammount of bytes to shift
	 * @see #shift(long)
	 */
	public void shift(long offset,long lenght){
		if(getMarker()>=size())
			return;
		if(offset == 0)
			return;
		if(lenght == 0)
			return;
		boolean startAtEnd = offset>0;
		long origin = getMarker();
		if(startAtEnd) {
			while (lenght > bufferSize) {
				jump(origin+(lenght -= bufferSize));
				shift(offset, bufferSize);
			}
		}else{
			while (lenght > bufferSize){
				shift(offset,bufferSize);
				skip(offset+bufferSize);
				lenght-=bufferSize;
			}
		}
		jump(origin);
		byte[] dataToShift = readNext(lenght);
		jump(origin+offset);
		write(dataToShift);
		jump(origin+offset);
	}

	@Override
	public String toString() {
		long originMarker = getMarker();
		jump(0);
		final StringBuffer sb = new StringBuffer(getClass().getSimpleName()+"->");
		sb.append('[');
		for (int i = 0; i < size(); ++i)
			sb.append(i == 0 ? "" : ", ").append(i == originMarker?"> ":"").append(read(i));
		sb.append(']');
		jump(originMarker);
		return sb.toString();
	}

	public InputStream openReadStream(){
		return new DataStoreInputStream();
	}
	public OutputStream openWritingStream(){
		return new DataStoreOutputStream();
	}
	public OutputStream openShiftWritingStream(){
		return new ShiftingOutputStream();
	}

	public Stream<Byte> stream(){
		long start;
		byte[] data = read(start = getMarker(),	size());
		jump(start);
		Byte[] wrapped = new Byte[data.length];
		for (int i = 0; i < data.length; i++) {
			wrapped[i] = data[i];
		}
		return Arrays.stream(wrapped);
	}
}
