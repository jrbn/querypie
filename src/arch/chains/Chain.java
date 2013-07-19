package arch.chains;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.actions.ActionsProvider;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;
import arch.data.types.Tuple;
import arch.data.types.bytearray.BDataInput;
import arch.data.types.bytearray.BDataOutput;
import arch.storage.Writable;
import arch.utils.Consts;
import arch.utils.Utils;

/**
 * 
 * 8 bytes: submission ID the chain belongs to 8 bytes: chain ID 8 bytes: parent
 * chain ID 4 bytes: n children 4 bytes: replicated factor 1 byte: input layer
 * to consider (0 is the default) 1 byte: flag to exclude the execution of the
 * fixed chain
 * 
 * @author jacopo
 * 
 */

public class Chain extends Writable {

	static final Logger log = LoggerFactory.getLogger(Chain.class);

	private int startingPosition = Consts.CHAIN_RESERVED_SPACE;
	private int bufferSize = Consts.CHAIN_RESERVED_SPACE;
	private static byte[] zeroBuf = new byte[Consts.CHAIN_RESERVED_SPACE];

	private final byte[] buffer = new byte[Consts.CHAIN_SIZE];
	private final Tuple inputTuple = new Tuple();

	private final BDataOutput cos = new BDataOutput(buffer);

	public void init(String[] availableActions) {
		System.arraycopy(zeroBuf, 0, buffer, 0, Consts.CHAIN_RESERVED_SPACE);
		startingPosition = Consts.CHAIN_RESERVED_SPACE;
		if (availableActions == null) {
			Utils.encodeInt(buffer, 35, 0);
		} else {
			// Sort the strings to save space
			String list = "";

			Arrays.sort(availableActions);
			String nameLastPackage = "";
			for (String action : availableActions) {
				String packageName = action.substring(0,
						action.lastIndexOf('.'));
				if (packageName.equals(nameLastPackage)) {
					list += "," + action.substring(action.lastIndexOf('.') + 1);
				} else {
					list += ":" + action;
					nameLastPackage = packageName;
				}
			}

			if (list.startsWith(",") || list.startsWith(":")) {
				list = list.substring(1);
			}

			byte[] toArray = list.getBytes();
			Utils.encodeInt(buffer, 35, toArray.length);
			System.arraycopy(toArray, 0, buffer, 39, toArray.length);
			startingPosition += toArray.length;
		}
		bufferSize = startingPosition;
		inputTuple.clear();
	}

	public String[] getAvailableActions() {
		int size = Utils.decodeInt(buffer, 35);

		if (size != 0) {
			ArrayList<String> classes = new ArrayList<String>();
			String list = new String(buffer, 39, size);
			String[] blocks = list.split(":");
			for (String block : blocks) {
				String[] names = block.split(",");
				String packageName = names[0].substring(0,
						names[0].lastIndexOf("."));
				classes.add(names[0]);
				for (int i = 1; i < names.length; ++i) {
					classes.add(packageName + "." + names[i]);
				}
			}
			return classes.toArray(new String[classes.size()]);
		}
		return null;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		startingPosition = input.readInt();
		bufferSize = input.readInt();
		input.readFully(buffer, 0, bufferSize);
		inputTuple.readFrom(input);
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(startingPosition);
		output.writeInt(bufferSize);
		output.write(buffer, 0, bufferSize);
		inputTuple.writeTo(output);
	}

	@Override
	public int bytesToStore() {
		return bufferSize + 4 + inputTuple.bytesToStore();
	}

	public void setSubmissionNode(int nodeId) {
		Utils.encodeInt(buffer, 0, nodeId);
	}

	public int getSubmissionNode() {
		return Utils.decodeInt(buffer, 0);
	}

	public void setSubmissionId(int submissionId) {
		Utils.encodeInt(buffer, 4, submissionId);
	}

	public int getSubmissionId() {
		return Utils.decodeInt(buffer, 4);
	}

	public void setChainId(long chainId) {
		Utils.encodeLong(buffer, 8, chainId);
	}

	public long getChainId() {
		return Utils.decodeLong(buffer, 8);
	}

	public void setParentChainId(long chainId) {
		Utils.encodeLong(buffer, 16, chainId);
	}

	public long getParentChainId() {
		return Utils.decodeLong(buffer, 16);
	}

	public void setChainChildren(int chainChildren) {
		Utils.encodeInt(buffer, 24, chainChildren);
	}

	public int getChainChildren() {
		return Utils.decodeInt(buffer, 24);
	}

	public int getReplicatedFactor() {
		return Utils.decodeInt(buffer, 28);
	}

	public void setReplicatedFactor(int factor) {
		Utils.encodeInt(buffer, 28, factor);
	}

	public void setInputLayerId(int id) {
		buffer[32] = (byte) id;
	}

	public int getInputLayerId() {
		return buffer[32];
	}

	public void setExcludeExecution(boolean value) {
		buffer[33] = (byte) (value ? 1 : 0);
	}

	public boolean getExcludeExecution() {
		return buffer[33] == 1 ? true : false;
	}

	public void setCustomFlag(byte value) {
		buffer[34] = value;
	}

	public int getCustomFlag() {
		return buffer[34];
	}

	// public void setExpansionComplete(boolean value) {
	// buffer[34] = (byte) (value ? 1 : 0);
	// }
	//
	// public boolean getExpansionComplete() {
	// return buffer[34] == 1 ? true : false;
	// }

	public void setInputTuple(Tuple tuple) throws Exception {
		if (tuple != null) {
			tuple.copyTo(inputTuple);
		}
	}

	public void getInputTuple(String prefixAction, Tuple tuple)
			throws Exception {
		// Find the first action of the action and read the output tuple.
		int bufferSize = this.bufferSize;
		// String listActions = "";
		while (bufferSize > startingPosition) {
			int size = bufferSize;
			int sizeName = Utils.decodeInt(buffer, size - 4);
			String nameAction = new String(buffer, size - sizeName - 4,
					sizeName);
			// listActions += nameAction + " ";
			if (nameAction.indexOf(prefixAction) != -1) {
				size -= sizeName + 4;
				int sizeTuple = Utils.decodeInt(buffer, size - 8);
				BDataInput cin = new BDataInput(buffer);
				cin.setCurrentPosition(size - sizeTuple - 8);
				tuple.readFrom(cin);
				return;
			}
			bufferSize -= Utils.decodeInt(buffer, bufferSize - sizeName - 8)
					+ sizeName + 4;
		}

		throw new Exception("Not a suitable action: ");

	}

	public void getInputTuple(Tuple tuple) throws Exception {
		inputTuple.copyTo(tuple);
	}

	public void addAction(Action action, Tuple tuple, Object... params)
			throws Exception {
		addAction(action.getClass().getName(), tuple, params);
	}

	public void addAction(String action, Tuple tuple, Object... params)
			throws Exception {

		// First write the params
		int sizeAction = 0;
		int i = 0;
		for (i = 0; params != null && i < params.length; ++i) {
			Object param = params[i];
			if (param instanceof Long) {
				Utils.encodeLong(buffer, bufferSize + sizeAction, (Long) param);
				buffer[bufferSize + sizeAction + 8] = 1;
				sizeAction += 9;
			} else if (param instanceof Integer) {
				Utils.encodeInt(buffer, bufferSize + sizeAction,
						(Integer) param);
				buffer[bufferSize + sizeAction + 4] = 2;
				sizeAction += 5;
			} else if (param instanceof Boolean) {
				buffer[bufferSize + sizeAction] = (byte) (((Boolean) param) ? 1
						: 0);
				buffer[bufferSize + sizeAction + 1] = 3;
				sizeAction += 2;
			} else if (param instanceof String) {
				byte[] bs = ((String) param).getBytes();
				System.arraycopy(bs, 0, buffer, bufferSize + sizeAction,
						bs.length);
				Utils.encodeInt(buffer, bufferSize + sizeAction + bs.length,
						bs.length);
				buffer[bufferSize + sizeAction + bs.length + 4] = 4;
				sizeAction += bs.length + 5;
			} else {
				log.error("Wrong parameter!", new Throwable());
			}
		}

		buffer[bufferSize + sizeAction] = (byte) i;
		bufferSize += sizeAction + 1;

		// Record the input tuple to detect loops
		int sizeTuple = 0;
		if (tuple != null) {
			sizeTuple = tuple.bytesToStore();
			cos.setCurrentPosition(bufferSize);
			tuple.writeTo(cos);
		} else {
			sizeTuple = Tuple.EMPTY_TUPLE.bytesToStore();
			cos.setCurrentPosition(bufferSize);
			Tuple.EMPTY_TUPLE.writeTo(cos);
		}
		bufferSize += sizeTuple;
		Utils.encodeInt(buffer, bufferSize, sizeTuple);

		sizeAction += sizeTuple + 4;
		// Write action id
		Utils.encodeInt(buffer, bufferSize + 4, sizeAction + 5);
		byte[] a = action.getBytes();
		System.arraycopy(a, 0, buffer, bufferSize + 8, a.length);
		Utils.encodeInt(buffer, bufferSize + 8 + a.length, a.length);
		bufferSize += 12 + a.length;

		/***** UPDATE THE INPUT TUPLE *****/
		if (tuple != null) {
			setInputTuple(tuple);
		}
	}

	// Check whether the action was called on this input
	public boolean detectLoopInputAction(String prefixPreviousAction,
			String action, Tuple tuple, int positionsToCheck,
			int valueFirstParam, ActionContext context) {
		try {
			int tmpSize = bufferSize;

			while (tmpSize > startingPosition) {
				int sizeName = Utils.decodeInt(buffer, tmpSize - 4);
				String actionName = new String(buffer, tmpSize - sizeName - 4,
						sizeName);

				if (actionName.equals(action)) {

					if (tuple == null) {
						return true;
					}

					int lengthTuple = Utils.decodeInt(buffer, tmpSize - 12
							- sizeName);
					int positionTuple = tmpSize - lengthTuple - 12 - sizeName;

					if (valueFirstParam == -1
							|| buffer[positionTuple - 3] == valueFirstParam) {

						boolean found = false;
						int ppositionTuple = -1;

						if (prefixPreviousAction == null) {
							// Need to check the tuple of the previous action
							int previous = tmpSize
									- (Utils.decodeInt(buffer, tmpSize - 8
											- sizeName) + 4 + sizeName);
							if (previous > startingPosition) {
								int previousSize = Utils.decodeInt(buffer,
										previous - 4);
								int plengthTuple = Utils.decodeInt(buffer,
										previous - 12 - previousSize);
								ppositionTuple = previous - plengthTuple - 12
										- previousSize;
								found = true;
							}
						} else {
							// Go back until we reach an action that starts with
							// the prefix
							int previousStart = tmpSize
									- (Utils.decodeInt(buffer, tmpSize - 8
											- sizeName) + 4 + sizeName);
							if (previousStart > startingPosition) {
								do {
									int pSizeName = Utils.decodeInt(buffer,
											previousStart - 4);
									String pActionName = new String(buffer,
											previousStart - pSizeName - 4,
											pSizeName);
									if (pActionName
											.indexOf(prefixPreviousAction) != -1) {
										int plengthTuple = Utils.decodeInt(
												buffer, previousStart - 12
														- pSizeName);
										ppositionTuple = previousStart
												- plengthTuple - 12 - pSizeName;
										found = true;
										break;
									}
									previousStart = previousStart
											- (Utils.decodeInt(buffer,
													previousStart - 8
															- pSizeName) + 4 + pSizeName);
								} while (previousStart > startingPosition);
							}

						}

						/***** new version *****/
						if (found) {
							boolean equal = true;

							for (int i = 0; equal && i < positionsToCheck; ++i) {
								SimpleData data1 = Tuple.getField(buffer,
										ppositionTuple, i,
										context.getDataProvider());

								if (data1 != null) {
									SimpleData data2 = context
											.getDataProvider().get(
													tuple.getType(i));
									tuple.get(data2, i);

									if (!data1.equals(data2, context)) {
										equal = false;
									}

									context.getDataProvider().release(data1);
									context.getDataProvider().release(data2);
								}
							}

							if (equal) {
								return true;
							}
						} else {
							throw new Exception(
									"Not found a proper suitable action");
						}
					}
				}
				tmpSize -= Utils.decodeInt(buffer, tmpSize - 8 - sizeName) + 4
						+ sizeName;
			}

		} catch (Exception e) {
			log.error("Failed comparison", e);
		}

		return false;

	}

	public boolean detectLoop(String prefixPreviousAction, Tuple tuple,
			int positionsToCheck, ActionContext context) {
		try {
			int tmpSize = bufferSize;

			boolean neverFound = true;
			boolean found = false;

			while (tmpSize > startingPosition) {
				int sizeName = Utils.decodeInt(buffer, tmpSize - 4);
				String actionName = new String(buffer, tmpSize - sizeName - 4,
						sizeName);

				if (actionName.indexOf(prefixPreviousAction) != -1 || found) {
					if (neverFound) {
						neverFound = false;
					} else /* if (!found) */{
						int lengthTuple = Utils.decodeInt(buffer, tmpSize - 12
								- sizeName);
						int positionTuple = tmpSize - lengthTuple - 12
								- sizeName;

						boolean equal = true;
						for (int i = 0; equal && i < positionsToCheck; ++i) {
							SimpleData data1 = Tuple
									.getField(buffer, positionTuple, i,
											context.getDataProvider());

							if (data1 != null) {
								SimpleData data2 = context.getDataProvider()
										.get(tuple.getType(i));
								tuple.get(data2, i);

								if (!data1.equals(data2, context)) {
									equal = false;
								}

								context.getDataProvider().release(data1);
								context.getDataProvider().release(data2);
							}
						}

						// Move to the action immediately before and return
						// the position of the parameters
						if (equal) {
							found = true;
							return true;
						}
						// } else {
						// // Return the position of the parameters
						// int lengthTuple = Utils.decodeInt(buffer, tmpSize -
						// 12
						// - sizeName);
						// return tmpSize - lengthTuple - 13 - sizeName;
					}
				}
				tmpSize -= Utils.decodeInt(buffer, tmpSize - 8 - sizeName) + 4
						+ sizeName;
			}

		} catch (Exception e) {
			log.error("Failed comparison", e);
		}
		return false;
	}

	public int getRawSize() {
		return bufferSize;
	}

	public void setRawSize(int size) {
		bufferSize = size;
	}

	protected void deleteLastAction() {
		int sizeClass = Utils.decodeInt(buffer, bufferSize - 4);
		bufferSize -= Utils.decodeInt(buffer, bufferSize - sizeClass - 8)
				+ sizeClass + 4;
	}

	public int getChainDetails(String[] actionNames, Object[][] params,
			int[] rawSizes) {
		// Start from the top
		int tmpSize = bufferSize;
		int i = 0;
		while (tmpSize > startingPosition) {
			tmpSize -= 4;
			int size = Utils.decodeInt(buffer, tmpSize);
			actionNames[i] = new String(buffer, tmpSize - size, size);
			// chainIDs[i] = Utils.decodeInt(byteBuffer, tmpSize);
			tmpSize -= 8 + size;
			int sizeTuple = Utils.decodeInt(buffer, tmpSize);
			tmpSize -= 1 + sizeTuple;

			// Get params
			int n = buffer[tmpSize];
			while (n-- > 0) {
				switch (buffer[--tmpSize]) {
				case 1:
					tmpSize -= 8;
					params[i][n] = Utils.decodeLong(buffer, tmpSize);
					break;
				case 2:
					tmpSize -= 4;
					params[i][n] = Utils.decodeInt(buffer, tmpSize);
					break;
				case 3:
					tmpSize--;
					params[i][n] = (buffer[tmpSize] == 1) ? true : false;
					break;
				case 4:
					tmpSize -= 4;
					int len = Utils.decodeInt(buffer, tmpSize);
					params[i][n] = new String(buffer, tmpSize - len, len);
					tmpSize -= len;
					break;
				}
			}
			rawSizes[i] = tmpSize;
			i++;
		}

		return i;
	}

	public void copyTo(Chain newChain) {
		newChain.startingPosition = startingPosition;
		newChain.bufferSize = bufferSize;
		System.arraycopy(buffer, 0, newChain.buffer, 0, bufferSize);
		inputTuple.copyTo(newChain.inputTuple);
	}

	public void generateChild(ActionContext context, Chain newChain) {
		copyTo(newChain);
		newChain.setParentChainId(this.getChainId());
		newChain.setChainId(context.getNewChainID());
		newChain.setChainChildren(0);
		setChainChildren(getChainChildren() + 1);
	}

	public String toString(ActionsProvider ap, DataProvider dp, int verbose) {
		try {
			// Parse the chain and return the list of actions
			int originalSize = bufferSize;
			Tuple tuple = new Tuple();
			BDataInput cin = new BDataInput(buffer);
			getInputTuple(tuple);

			String details = " ChainId: " + getChainId() + " parent: "
					+ getParentChainId();

			if (verbose < 2) {
				details += " Input: (" + tuple.toString(dp) + ") ";
			}

			if (verbose < 1) {
				details += "submissionId: " + getSubmissionId()
						+ " excludeExecution: " + getExcludeExecution()
						+ " rep: " + getReplicatedFactor() + " children: "
						+ getChainChildren();
			}

			while (bufferSize > startingPosition) {
				int size = bufferSize;
				int sizeName = Utils.decodeInt(buffer, size - 4);
				String nameAction = new String(buffer, size - sizeName - 4,
						sizeName);
				size -= sizeName + 4;
				int sizeAction = Utils.decodeInt(buffer, size - 4);
				int sizeTuple = Utils.decodeInt(buffer, size - 8);
				cin.setCurrentPosition(size - sizeTuple - 8);
				int tmpSize = size - sizeTuple - 8 - 1;
				int n = buffer[tmpSize];
				Object[] params = new Object[n];
				while (n-- > 0) {
					switch (buffer[--tmpSize]) {
					case 1:
						tmpSize -= 8;
						params[n] = Utils.decodeLong(buffer, tmpSize);
						break;
					case 2:
						tmpSize -= 4;
						params[n] = Utils.decodeInt(buffer, tmpSize);
						break;
					case 3:
						tmpSize--;
						params[n] = (buffer[tmpSize] == 1) ? true : false;
						break;
					case 4:
						tmpSize -= 4;
						int len = Utils.decodeInt(buffer, tmpSize);
						params[n] = "\""
								+ new String(buffer, tmpSize - len, len) + "\"";
						tmpSize -= len;
						break;
					}
				}
				size -= sizeAction;
				tuple.readFrom(cin);

				if (verbose == -3) {
					if (nameAction.indexOf("Rule") != -1) {
						details += "\n" + nameAction + " ";
						details += "(" + tuple.toString(dp) + ") ";
						details += Arrays.toString(params);
					}
				} else {

					if (verbose < 2) {
						details += nameAction + " ";
					}
					if (verbose < 1) {
						details += "(" + tuple.toString(dp) + ") ";
					}

					if (verbose < 0) {
						details += Arrays.toString(params) + " ";
					}
				}

				deleteLastAction();
			}

			bufferSize = originalSize;
			return details;
		} catch (Exception e) {
			log.error("Problems parsing the chain", e);
		}
		return null;
	}

	public int countNumberActions(String prefix) {
		int count = 0;
		try {
			int tmpSize = bufferSize;
			while (tmpSize > startingPosition) {
				int sizeName = Utils.decodeInt(buffer, tmpSize - 4);
				String actionName = new String(buffer, tmpSize - sizeName - 4,
						sizeName);
				if (actionName.indexOf(prefix) != -1) {
					count++;
				}
				tmpSize -= Utils.decodeInt(buffer, tmpSize - 8 - sizeName) + 4
						+ sizeName;
			}
		} catch (Exception e) {
			log.error("Failed counting the triples", e);
		}

		return count;
	}
}
