package nl.vu.cs.querypie.sparql;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;

public class Project extends Action {
	
	public static final int IA_POS = 0;
	
	private int[] positions;
	private SimpleData[] outputTuple;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(IA_POS, "IA_POS", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		positions = getParamIntArray(IA_POS);
		outputTuple = new SimpleData[positions.length];
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		for(int i = 0; i < positions.length; ++i) {
			outputTuple[i] = tuple.get(positions[i]);
		}
		actionOutput.output(outputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		positions = null;
		outputTuple = null;
	}
}
