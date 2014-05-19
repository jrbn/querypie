package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;

public class RuleGetPattern extends Action {

	public static final int B_CONVERGE = 0;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_CONVERGE, "B_CONVERGE", false, false);
	}	

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (inputTuple.getNElements() == 4) {
			inputTuple.set(inputTuple.get(0), inputTuple.get(1),
					inputTuple.get(2));
		} else {
			output.output(inputTuple);
		}
	}
}
