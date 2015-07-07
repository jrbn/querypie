package nl.vu.cs.querypie.sparql;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class SPARQLPrintOutput extends Action {

	private TString line = new TString();

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// create the line
		String l = "";
		for (int i = 0; i < tuple.getNElements(); ++i) {
			l += tuple.get(i) + " ";
		}
		line.setValue(l.trim());
		actionOutput.output(line);
	}

}
