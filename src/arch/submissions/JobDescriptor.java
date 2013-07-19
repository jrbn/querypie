package arch.submissions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import arch.actions.Action;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.storage.Writable;
import arch.utils.Consts;

public class JobDescriptor extends Writable {

	int inputLayer = Consts.DEFAULT_INPUT_LAYER_ID;
	Tuple inputTuple = new Tuple();
	List<String> action = new ArrayList<String>();
	List<Tuple> tuples = new ArrayList<Tuple>();
	String availableRules = null;
	boolean notExecuteMainProgram = false;
	boolean waitForStatistics = true;
	boolean printIntermediateStats = false;
	boolean printStatistics = true;
	int assignedBucket = -1;

	public void addAction(Class<? extends Action> clazz) throws Exception {
		action.add(clazz.getCanonicalName());
		tuples.add(null);
	}

	public void addAction(Class<? extends Action> clazz, Tuple tuple)
			throws Exception {
		action.add(clazz.getCanonicalName());
		tuples.add(tuple);
	}

	public void setAvailableRules(String availableRules) {
		this.availableRules = availableRules;
	}

	public void setInputTuple(Tuple tuple) {
		inputTuple = tuple;
	}

	public void setInputLayer(int idLayer) {
		inputLayer = idLayer;
	}

	public void setPrintIntermediateStats(boolean value) {
		printIntermediateStats = value;
	}

	public void excludeExecutionMainChain(boolean value) {
		notExecuteMainProgram = value;
	}

	public boolean getExcludeMainChain() {
		return notExecuteMainProgram;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		inputLayer = input.readByte();
		inputTuple.readFrom(input);
		int n = input.readInt();
		action.clear();
		tuples.clear();
		for (int i = 0; i < n; ++i) {
			action.add(input.readUTF());
		}
		for (int i = 0; i < n; ++i) {
			if (input.readByte() == 1) {
				Tuple tuple = new Tuple();
				tuple.readFrom(input);
				tuples.add(tuple);
			} else {
				tuples.add(null);
			}
		}
		availableRules = input.readUTF();
		notExecuteMainProgram = input.readBoolean();
		waitForStatistics = input.readBoolean();
		printIntermediateStats = input.readBoolean();
		printStatistics = input.readBoolean();
		assignedBucket = input.readInt();
	}

	public String toString(DataProvider dp) {
		return "input " + inputTuple.toString(dp) + " # actions "
				+ action.size() + " availRules " + availableRules
				+ " execMainProg " + notExecuteMainProgram;
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(inputLayer);
		inputTuple.writeTo(output);
		output.writeInt(action.size());
		for (String a : action) {
			output.writeUTF(a);
		}
		for (Tuple tuple : tuples) {
			if (tuple != null) {
				output.writeByte(1);
				tuple.writeTo(output);
			} else {
				output.writeByte(0);
			}
		}
		output.writeUTF(availableRules);
		output.writeBoolean(notExecuteMainProgram);
		output.writeBoolean(waitForStatistics);
		output.writeBoolean(printIntermediateStats);
		output.writeBoolean(printStatistics);
		output.writeInt(assignedBucket);
	}

	@Override
	public int bytesToStore() {
		return 0;
	}

	public void setAssignedOutputBucket(int bucket) {
		assignedBucket = bucket;
	}

	public void setWaitForStatistics(boolean waitForStatistics) {
		this.waitForStatistics = waitForStatistics;
	}

	public boolean getWaitForStatistics() {
		return waitForStatistics;
	}
}