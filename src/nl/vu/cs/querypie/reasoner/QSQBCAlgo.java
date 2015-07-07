package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoning.expand.QSQEvaluateQuery;
import nl.vu.cs.querypie.storage.RDFTerm;

public class QSQBCAlgo extends Action {

	public static final String TREE_ID = "tree";

	public static final void applyTo(RDFTerm v1, RDFTerm v2, RDFTerm v3,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
		c.setParamString(QueryInputLayer.S_INPUTLAYER,
				DummyLayer.class.getName());
		c.setParamWritable(
				QueryInputLayer.W_QUERY,
				new nl.vu.cs.ajira.actions.support.Query(TupleFactory.newTuple(
						v1, v2, v3, new TInt(0))));
		actions.add(c);

		c = ActionFactory.getActionConf(QSQBCAlgo.class);
		actions.add(c);

		actions.add(ActionFactory.getActionConf(QSQRemoveLastTerm.class));
		
		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				RDFTerm.class.getName(), RDFTerm.class.getName(),
				RDFTerm.class.getName());
		actions.add(c);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		Ruleset.getInstance().setQsqEvaluation();
		long v1 = ((RDFTerm) tuple.get(0)).getValue();
		long v2 = ((RDFTerm) tuple.get(1)).getValue();
		long v3 = ((RDFTerm) tuple.get(2)).getValue();
		ActionSequence actions = new ActionSequence();
		QSQEvaluateQuery.evaluateRootQuery(actions, v1, v2, v3, context);
		actionOutput.branch(actions);
	}
}