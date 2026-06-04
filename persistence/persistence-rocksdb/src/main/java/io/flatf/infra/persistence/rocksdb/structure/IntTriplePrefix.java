package io.flatf.infra.persistence.rocksdb.structure;

import java.nio.ByteBuffer;

public class IntTriplePrefix {

	public static final int PrefixLength = Integer.BYTES * 3;

	public final int first;
	public final int second;
	public final int third;

	public final ByteBuffer byteBuffer;

	public IntTriplePrefix(int first, int second, int third) {
		this.first = first;
		this.second = second;
		this.third = third;
		this.byteBuffer = ByteBuffer.allocate(PrefixLength)
				.putInt(first).putInt(second).putInt(third);
	}

	public IntTriplePrefix(int first, int second, int third, int extLength) {
		this.first = first;
		this.second = second;
		this.third = third;
		this.byteBuffer = ByteBuffer.allocate(PrefixLength + extLength)
				.putInt(first).putInt(second).putInt(third);
	}

}
