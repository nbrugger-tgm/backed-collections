package com.niton.memory.direct.stores;

import com.niton.memory.direct.DataStore;
import com.niton.memory.direct.managed.*;

/**
 * A Datastore with NON variable size
 */
public abstract class FixedDataStore extends DataStore {

	private long end = 0;
	@Override
	public long size() {
		return Math.min(end,maxLength());
	}
	public abstract int maxLength();

	@Override
	protected byte[] innerRead(long from, long to) {
		if(from > maxLength() || to > maxLength())
			throw new Section.SegmentationFault(to+" is outside the readable area (0-"+maxLength()+")");
		return fixedInnerRead(from,to);
	}

	protected abstract byte[] fixedInnerRead(long from, long to);

	@Override
	protected void innerWrite(byte[] data, long from, long to) {
		if(to > maxLength())
			throw new MemoryOverflowException(maxLength(), to);
		fixedInnerWrite(data,from,to);
		end = Math.max(to,end);
	}

	@Override
	public long cut(long from) {
		end = from;
		jump(end);
		return maxLength()-from;
	}

	protected abstract void fixedInnerWrite(byte[] data, long from, long to);


	@Override
	public String toString() {
		long originMarker = getMarker();
		final StringBuffer sb = new StringBuffer(getClass().getSimpleName()+"->");
			sb.append('[');
			for (int i = 0; i < size(); ++i)
				sb.append(i == 0 ? "" : ", ").append(i == originMarker?"> ":"").append(read(i));
			sb.append(']');
			jump(originMarker);
		return sb.toString();
	}
	public static class MemoryOverflowException extends RuntimeException{
		public MemoryOverflowException(long maxSize,long writePosition) {
			super("You DataStore is out of Memory. DataStore:"+VirtualMemory.humanReadableByteCountSI(maxSize)+" Write address:"+VirtualMemory.humanReadableByteCountSI(writePosition));
		}
	}
}
