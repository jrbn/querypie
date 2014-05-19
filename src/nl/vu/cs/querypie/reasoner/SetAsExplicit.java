package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.Tuple;

public class SetAsExplicit extends Action {

	private final static TBoolean derived = new TBoolean(false);
	
	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple.get(0), tuple.get(1), tuple.get(2), derived);
	}

}
