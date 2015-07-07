package nl.vu.cs.querypie.reasoner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoning.expand.Tree;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;
import nl.vu.cs.querypie.utils.TripleBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddIntermediateTriples extends Action {

	static final Logger log = LoggerFactory
			.getLogger(AddIntermediateTriples.class);

	// Fields of the tuple parameter.
	public static final int L_FIELD1 = 0;
	public static final int L_FIELD2 = 1;
	public static final int L_FIELD3 = 2;
	public static final int I_QUERYID = 3;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(L_FIELD1, "L_FIELD1", 0, true);
		conf.registerParameter(L_FIELD2, "L_FIELD2", 0, true);
		conf.registerParameter(L_FIELD3, "L_FIELD3", 0, true);
		conf.registerParameter(I_QUERYID, "I_QUERYID", 0, true);
	}

	public static void applyTo(long s, long p, long o, int queryid,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(AddIntermediateTriples.class);
		c.setParamLong(L_FIELD1, s);
		c.setParamLong(L_FIELD2, p);
		c.setParamLong(L_FIELD3, o);
		c.setParamInt(I_QUERYID, queryid);
		actions.add(c);
	}

	InMemoryTripleContainer input = null;
	private TripleBuffer buffer = null;
	private boolean isUpdated;
        private int queryId;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// count = 0;
		input = (InMemoryTripleContainer) context
				.getObjectFromCache("inputIntermediateTuples");

		long[] query = new long[3];
		query[0] = getParamLong(L_FIELD1);
		query[1] = getParamLong(L_FIELD2);
		query[2] = getParamLong(L_FIELD3);
                queryId = getParamInt(I_QUERYID);
		int nconsts = 0;
		int[] posConsts = new int[3];
		long[] valueConsts = new long[3];
		for (int i = 0; i < 3; ++i) {
			long v = query[i];
			if (v >= 0) {
				valueConsts[nconsts] = v;
				posConsts[nconsts++] = i;
			}
		}
		if (nconsts == 3) {
			buffer = TripleBuffer.getList(posConsts, valueConsts);
		} else {
			buffer = TripleBuffer.getList(Arrays.copyOf(posConsts, nconsts),
					Arrays.copyOf(valueConsts, nconsts));
		}

		isUpdated = false;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		long a = ((RDFTerm) inputTuple.get(0)).getValue();
		long b = ((RDFTerm) inputTuple.get(1)).getValue();
		long c = ((RDFTerm) inputTuple.get(2)).getValue();

		if (!isUpdated && ((TBoolean) inputTuple.get(3)).getValue()) {
			isUpdated = true;
		}

		if (input == null
				|| !input.containsTriple(a, b, c)) {

			buffer.add(a, b, c);
			output.output(inputTuple);
			if (isUpdated) {
				((Tree) context.getObjectFromCache("tree")).getQuery(
						queryId).setUpdated();
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void stopProcess(ActionContext context, ActionOutput output) {

		if (buffer != null && buffer.getNElements() > 0) {
			synchronized (AddIntermediateTriples.class) {
				List<TripleBuffer> list = (List<TripleBuffer>) context
						.getObjectFromCache("intermediateTuples");
				if (list == null) {
					list = new ArrayList<TripleBuffer>();
					context.putObjectInCache("intermediateTuples", list);
				}
				list.add(buffer);
			}
		}
		buffer = null;
		input = null;
	}
}
