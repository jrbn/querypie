package nl.vu.cs.querypie.utils;

import gnu.trove.list.array.TLongArrayList;

public abstract class TripleBuffer {

	protected final TLongArrayList list = new TLongArrayList();
	protected final long[] tuple = new long[3];

	public abstract int getNElements();

	public abstract void add(final long a, final long b, final long c);

	public abstract long[] get(final int idx);

	public final static TripleBuffer getList(final int[] posConstants,
			long[] valueConstants) throws Exception {
		int sizeTuple = 3 - posConstants.length;

		switch (sizeTuple) {
		case 0:
			return new AllConsts(valueConstants[0], valueConstants[1],
					valueConstants[2]);
		case 1:
			if (posConstants[0] == 0) {
				if (posConstants[1] == 1) {
					return new O(valueConstants[0], valueConstants[1]);
				} else { // Must be 2
					return new P(valueConstants[0], valueConstants[1]);
				}
			} else { // Second must be 2
				return new S(valueConstants[0], valueConstants[1]);
			}
		case 2:
			if (posConstants[0] == 0) {
				return new PO(valueConstants[0]);
			} else if (posConstants[0] == 1) {
				return new OS(valueConstants[0]);
			} else {
				return new SP(valueConstants[0]);
			}
		case 3:
			return new SPO();
		}
		throw new Exception("Should never happen");
	}

	private static class AllConsts extends TripleBuffer {

		private boolean ok = false;

		public AllConsts(final long s, final long p, final long o) {
			tuple[0] = s;
			tuple[1] = p;
			tuple[2] = o;
			ok = false;
		}

		@Override
		public int getNElements() {
			if (ok) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public void add(long a, long b, long c) {
			assert (a == tuple[0] && b == tuple[1] && c == tuple[2]);
			ok = true;
		}

		@Override
		public long[] get(int idx) {
			return tuple;
		}

	}

	private static class SPO extends TripleBuffer {
		@Override
		public void add(final long a, final long b, final long c) {
			list.add(a);
			list.add(b);
			list.add(c);
		}

		@Override
		public long[] get(int idx) {
			int startIdx = idx * 3;
			tuple[0] = list.get(startIdx++);
			tuple[1] = list.get(startIdx++);
			tuple[2] = list.get(startIdx);
			return tuple;
		}

		@Override
		public int getNElements() {
			return list.size() / 3;
		}
	}

	private static abstract class TripleBuffer2 extends TripleBuffer {

		@Override
		public int getNElements() {
			return list.size() / 2;
		}
	}

	private static abstract class TripleBuffer1 extends TripleBuffer {

		@Override
		public int getNElements() {
			return list.size();
		}
	}

	private static class PO extends TripleBuffer2 {

		PO(final long s) {
			tuple[0] = s;
		}

		@Override
		public void add(final long a, final long b, final long c) {
			list.add(b);
			list.add(c);
		}

		@Override
		public long[] get(int idx) {
			int startIdx = idx * 2;
			tuple[1] = list.get(startIdx++);
			tuple[2] = list.get(startIdx);
			return tuple;
		}
	}

	private static class OS extends TripleBuffer2 {

		OS(final long p) {
			tuple[1] = p;
		}

		@Override
		public void add(final long a, final long b, final long c) {
			list.add(c);
			list.add(a);
		}

		@Override
		public long[] get(int idx) {
			int startIdx = idx * 2;
			tuple[2] = list.get(startIdx++);
			tuple[0] = list.get(startIdx);
			return tuple;
		}
	}

	private static class SP extends TripleBuffer2 {

		SP(final long o) {
			tuple[2] = o;
		}

		@Override
		public void add(final long a, final long b, final long c) {
			list.add(a);
			list.add(b);
		}

		@Override
		public long[] get(int idx) {
			int startIdx = idx * 2;
			tuple[0] = list.get(startIdx++);
			tuple[1] = list.get(startIdx);
			return tuple;
		}
	}

	private static class S extends TripleBuffer1 {

		S(final long p, final long o) {
			tuple[1] = p;
			tuple[2] = o;
		}

		@Override
		public void add(final long a, final long b, final long c) {
			list.add(a);
		}

		@Override
		public long[] get(int idx) {
			tuple[0] = list.get(idx);
			return tuple;
		}
	}

	private static class P extends TripleBuffer1 {

		P(final long s, final long o) {
			tuple[0] = s;
			tuple[2] = o;
		}

		@Override
		public void add(final long a, final long b, final long c) {
			list.add(b);
		}

		@Override
		public long[] get(int idx) {
			tuple[1] = list.get(idx);
			return tuple;
		}
	}

	private static class O extends TripleBuffer1 {

		O(final long s, final long p) {
			tuple[0] = s;
			tuple[1] = p;
		}

		@Override
		public void add(final long a, final long b, final long c) {
			list.add(c);
		}

		@Override
		public long[] get(int idx) {
			tuple[2] = list.get(idx);
			return tuple;
		}
	}
}
