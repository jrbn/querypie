package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor1;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor2;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor3;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor4;
import nl.vu.cs.querypie.reasoning.expand.ExpandQuery;
import nl.vu.cs.querypie.reasoning.expand.QueryNode;
import nl.vu.cs.querypie.reasoning.expand.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

public class ReasoningUtils {
	public static ActionSequence getResultsQuery(ActionSequence chain,
			Tuple tuple, boolean converge) throws ActionNotConfiguredException {
		// Get the pattern

		Query q = new Query(tuple);
		ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
		c.setParamWritable(QueryInputLayer.W_QUERY, q);
		c.setParamString(QueryInputLayer.S_INPUTLAYER,
				QueryInputLayer.DEFAULT_LAYER);
		chain.add(c);

		if (converge) {
			c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);
			chain.add(c);
			chain.add(ActionFactory.getActionConf(RemoveDuplicates.class));
		}

		return chain;
	}

	public static final Class<?>[] ruleClasses = { null, RuleExecutor1.class,
			RuleExecutor2.class, RuleExecutor3.class, RuleExecutor4.class };

	public static final void generate_new_chain(ActionOutput output, Rule rule,
			int stratg_id, boolean group_to_single_node, QueryNode rawTuple,
			int currentPattern, QueryNode head, ActionContext context,
			int list_id, boolean recursive, boolean cacheInput,
			String treeName, int idFilterValues) throws Exception {
		generate_new_chain(output, rule, stratg_id, group_to_single_node,
				rawTuple, currentPattern, head, context, list_id, recursive,
				cacheInput, TreeExpander.ALL, treeName, idFilterValues);
	}

	public static final void generate_new_chain(ActionOutput output, Rule rule,
			int stratg_id, boolean group_to_single_node, QueryNode rawTuple,
			int currentPattern, QueryNode head, ActionContext context,
			int list_id, boolean recursive, boolean cacheInput, int typeRules,
			String treeName, int idFilterValues) throws Exception {
		ActionSequence newChain = new ActionSequence();
		generate_new_chain(newChain, rule, stratg_id, group_to_single_node,
				rawTuple, currentPattern, head, context, list_id, recursive,
				cacheInput, typeRules, treeName, idFilterValues);
		output.branch(newChain);
	}

	public static final void applyRule(ActionSequence newChain, Rule rule,
			QueryNode head, QueryNode query, int stratg_id, int currentPattern,
			String treeName, int list_id, int idFilterValues, int ruleset)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(ruleClasses[rule.type]
				.getName());
		c.setParamInt(RuleExecutor1.I_RULEDEF, rule.id);
		c.setParamLong(RuleExecutor1.L_FIELD1, head.getS());
		c.setParamLong(RuleExecutor1.L_FIELD2, head.p);
		c.setParamLong(RuleExecutor1.L_FIELD3, head.o);
		c.setParamInt(RuleExecutor1.I_RULESET, ruleset);
		if (rule.type > 1) {
			c.setParamInt(RuleExecutor2.I_FILTERVALUESSET, idFilterValues);
		}
		if (rule.type > 2) {
			c.setParamInt(RuleExecutor3.I_STRAG_ID, stratg_id);
			c.setParamInt(RuleExecutor3.I_PATTERN_POS, currentPattern);
			c.setParamInt(RuleExecutor3.I_QUERY_ID, query.getId());
			c.setParamString(RuleExecutor3.S_TREENAME, treeName);
		}
		if (rule.type == 4) {
			c.setParamInt(RuleExecutor4.I_LIST_CURRENT, list_id);
		}
		newChain.add(c);
	}

	public static final void generate_new_chain(ActionSequence newChain,
			Rule rule, int stratg_id, boolean group_to_single_node,
			QueryNode rawTuple, int currentPattern, QueryNode head,
			ActionContext context, int list_id, boolean recursive,
			boolean cacheInput, int typeRules, String treeName,
			int idFilterValues) throws Exception {
		// Read from the dummy layer
		if (rawTuple.getS() == Schema.SCHEMA_SUBSET || recursive) {

			ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
			c.setParamString(QueryInputLayer.S_INPUTLAYER,
					DummyLayer.class.getName());
			c.setParamWritable(
					QueryInputLayer.W_QUERY,
					new nl.vu.cs.ajira.actions.support.Query(TupleFactory
							.newTuple(new RDFTerm(rawTuple.getS()),
									new RDFTerm(rawTuple.p), new RDFTerm(
											rawTuple.o),
											new TInt(rawTuple.getId()))));
			newChain.add(c);
		} else { // Read from the default layer
			ReasoningUtils.getResultsQuery(newChain, TupleFactory.newTuple(
					new RDFTerm(rawTuple.getS()), new RDFTerm(rawTuple.p),
					new RDFTerm(rawTuple.o)), false);
			newChain.add(ActionFactory.getActionConf(SetAsExplicit.class));
		}

		if (recursive && rawTuple.getS() != Schema.SCHEMA_SUBSET) {
			ActionConf c = ActionFactory.getActionConf(ExpandQuery.class);
			c.setParamBoolean(ExpandQuery.B_EXPLICIT, true);
			c.setParamInt(ExpandQuery.I_TYPE_RULES, typeRules);
			newChain.add(c);
		}

		if (group_to_single_node) {
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);
			newChain.add(c);
		}

		// if (rawTuple.s != Schema.SCHEMA_SUBSET && cacheInput) {
		// AddIntermediateTriples.applyTo(rawTuple.s, rawTuple.p, rawTuple.o,
		// newChain);
		// }

		applyRule(newChain, rule, head, rawTuple, stratg_id, currentPattern,
				treeName, list_id, idFilterValues, typeRules);

		AddIntermediateTriples.applyTo(head.getS(), head.p, head.o,
				head.getId(), newChain);
	}
}
