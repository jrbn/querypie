package nl.vu.cs.querypie.joins;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.google.common.primitives.Longs;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.LongMap;
import nl.vu.cs.querypie.storage.RDFTerm;

public class HashJoin extends Action {

	private static final class SequenceLongs {
		final long v1, v2;
		long v3;
		final int n;

		SequenceLongs(long v1, long v2, long v3) {
			this.v1 = v1;
			this.v2 = v2;
			this.v3 = v3;
			n = 3;
		}

		SequenceLongs(long v1, long v2) {
			this.v1 = v1;
			this.v2 = v2;
			n = 2;
		}

		public int hashCode() {
			if (n == 2) {
				return Longs.hashCode(v1) ^ Longs.hashCode(v2);
			} else {
				return Longs.hashCode(v1) ^ Longs.hashCode(v2)
						^ Longs.hashCode(v3);
			}
		}

		@Override
		public boolean equals(Object obj) {
			SequenceLongs o = (SequenceLongs) obj;
			if (n == 2) {
				return o.v1 == v1 && o.v2 == v2;
			} else {
				return o.v1 == v1 && o.v2 == v2 && o.v3 == v3;
			}
		}
	}

	public static final int S_TABLE = 0;
	public static final int IA_POS_TABLE = 1;
	public static final int IA_POS_INPUT = 2;
	public static final int IA_POS_COPYINPUT = 3;

	private int[] posFieldsToCopy;
	private int ncopies = 0;
	private int[] posJoinsTable;
	private int[] posJoinsInput;
	private int njoins = 0;

	private Table existingTable;
	private RDFTerm[] outputTuple;

	private Integer[] tableidx;
	private LongMap<Integer> singleKeyJoinMap;
	private Map<SequenceLongs, Integer> multiKeysJoinMap;

	String nameTable;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_TABLE, "S_TABLE", null, true);
		conf.registerParameter(IA_POS_TABLE, "IA_POS_TABLE", null, true);
		conf.registerParameter(IA_POS_INPUT, "IA_POS_INPUT", null, true);
		conf.registerParameter(IA_POS_COPYINPUT, "IA_POS_COPYINPUT", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		nameTable = getParamString(S_TABLE);
		posFieldsToCopy = getParamIntArray(IA_POS_COPYINPUT);
		ncopies = posFieldsToCopy.length;

		posJoinsTable = getParamIntArray(IA_POS_TABLE);
		posJoinsInput = getParamIntArray(IA_POS_INPUT);
		njoins = posJoinsInput.length;

		existingTable = (Table) context.getObjectFromCache(nameTable);
		outputTuple = new RDFTerm[existingTable.sizeRow() + ncopies];
		for (int i = 0; i < outputTuple.length; ++i) {
			outputTuple[i] = new RDFTerm();
		}
		tableidx = null;
		singleKeyJoinMap = null;
		multiKeysJoinMap = null;
	}

	private void prepareIndices() throws Exception {
		// Maybe in the future we can optimize it using a primitive type rather
		// than a Integer
		tableidx = new Integer[existingTable.size()];
		for (int i = 0; i < tableidx.length; ++i) {
			tableidx[i] = i * existingTable.sizeRow();
		}

		if (njoins == 0) {
			throw new Exception("Cartesian product is not (yet) supported");
		} else if (njoins == 1) {
			// Sort by the key
			final int posToCompare = posJoinsTable[0];
			Arrays.sort(tableidx, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return (int) (existingTable.get(o1 + posToCompare) - existingTable
							.get(o2 + posToCompare));
				}
			});
			singleKeyJoinMap = new LongMap<Integer>();
			long lastValue = existingTable.get(tableidx[0] + posToCompare);
			singleKeyJoinMap.put(lastValue, 0);
			for (int i = 1; i < tableidx.length; ++i) {
				long rowValue = existingTable.get(tableidx[i] + posToCompare);
				if (rowValue != lastValue) {
					singleKeyJoinMap.put(rowValue, i);
					lastValue = rowValue;
				}
			}
		} else {
			Arrays.sort(tableidx, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					if (njoins == 2) {
						int diff = (int) (existingTable.get(o1
								+ posJoinsTable[0]) - existingTable.get(o2
								+ posJoinsTable[0]));
						if (diff == 0) {
							diff = (int) (existingTable.get(o1
									+ posJoinsTable[1]) - existingTable.get(o2
									+ posJoinsTable[1]));
						}
						return diff;
					} else {
						int diff = (int) (existingTable.get(o1
								+ posJoinsTable[0]) - existingTable.get(o2
								+ posJoinsTable[0]));
						if (diff == 0) {
							diff = (int) (existingTable.get(o1
									+ posJoinsTable[1]) - existingTable.get(o2
									+ posJoinsTable[1]));
							if (diff == 0) {
								diff = (int) (existingTable.get(o1
										+ posJoinsTable[2]) - existingTable
										.get(o2 + posJoinsTable[2]));
							}
						}
						return diff;
					}
				}
			});
			multiKeysJoinMap = new HashMap<SequenceLongs, Integer>();
			for (int i = 0; i < tableidx.length; ++i) {
				boolean newKey = i == 0
						|| (njoins == 2 && (existingTable.get(tableidx[i]
								+ posJoinsTable[0]) != existingTable
								.get(tableidx[i - 1] + posJoinsTable[0]) || existingTable
								.get(tableidx[i] + posJoinsTable[1]) != existingTable
								.get(tableidx[i - 1] + posJoinsTable[1])))
						|| (njoins == 3 && (existingTable.get(tableidx[i]
								+ posJoinsTable[0]) != existingTable
								.get(tableidx[i - 1] + posJoinsTable[0])
								|| existingTable.get(tableidx[i]
										+ posJoinsTable[1]) != existingTable
										.get(tableidx[i - 1] + posJoinsTable[1]) || existingTable
								.get(tableidx[i] + posJoinsTable[2]) != existingTable
								.get(tableidx[i - 1] + posJoinsTable[2])));
				if (newKey) {
					if (njoins == 2) {
						multiKeysJoinMap.put(
								new SequenceLongs(existingTable.get(tableidx[i]
										+ posJoinsTable[0]), existingTable
										.get(tableidx[i] + posJoinsTable[1])),
								i);
					} else {

					}

				}
			}
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (tableidx == null) {
			prepareIndices();
		}

		// Perform the joins
		Integer positionRows = null;
		if (njoins == 1) {
			positionRows = singleKeyJoinMap.get(((RDFTerm) tuple
					.get(posJoinsInput[0])).getValue());
		} else if (njoins == 2) {
			positionRows = multiKeysJoinMap.get(new SequenceLongs(
					((RDFTerm) tuple.get(posJoinsInput[0])).getValue(),
					((RDFTerm) tuple.get(posJoinsInput[1])).getValue()));
		} else {
			positionRows = multiKeysJoinMap.get(new SequenceLongs(
					((RDFTerm) tuple.get(posJoinsInput[0])).getValue(),
					((RDFTerm) tuple.get(posJoinsInput[1])).getValue(),
					((RDFTerm) tuple.get(posJoinsInput[2])).getValue()));
		}

		if (positionRows != null) {
			// Materialize the join
			int startInput = outputTuple.length - ncopies;
			int lengthPreviousTable = startInput;
			for (int i = 0; i < ncopies; ++i) {
				outputTuple[startInput++].setValue(((RDFTerm) tuple
						.get(posFieldsToCopy[i])).getValue());
			}

			boolean continueMaterialization = true;
			while (continueMaterialization) {
				int row = tableidx[positionRows];
				for (int j = 0; j < lengthPreviousTable; ++j) {
					outputTuple[j].setValue(existingTable.get(row + j));
				}
				actionOutput.output(outputTuple);
				continueMaterialization = testNextRowShareSameKeys(++positionRows);
			}
		}
	}

	private boolean testNextRowShareSameKeys(int idx) {
		if (idx < tableidx.length) {
			switch (njoins) {
			case 1:
				return existingTable.get(tableidx[idx] + posJoinsTable[0]) == existingTable
						.get(tableidx[idx - 1] + posJoinsTable[0]);
			case 2:
				return existingTable.get(tableidx[idx] + posJoinsTable[0]) == existingTable
						.get(tableidx[idx - 1] + posJoinsTable[0])
						&& existingTable.get(tableidx[idx] + posJoinsTable[1]) == existingTable
								.get(tableidx[idx - 1] + posJoinsTable[1]);
			case 3:
				return existingTable.get(tableidx[idx] + posJoinsTable[0]) == existingTable
						.get(tableidx[idx - 1] + posJoinsTable[0])
						&& existingTable.get(tableidx[idx] + posJoinsTable[1]) == existingTable
								.get(tableidx[idx - 1] + posJoinsTable[1])
						&& existingTable.get(tableidx[idx] + posJoinsTable[2]) == existingTable
								.get(tableidx[idx - 1] + posJoinsTable[2]);
			}
		}
		return false;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		nameTable = null;
		posFieldsToCopy = null;
		existingTable = null;
		outputTuple = null;

		tableidx = null;
		singleKeyJoinMap = null;
		multiKeysJoinMap = null;
	}
}
