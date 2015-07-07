package nl.vu.cs.querypie.sparql;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Lock;
import nl.vu.cs.querypie.storage.TripleIterator;

public class EstimateCardinality extends Action {

	public static String LOCK = "estimationLock";

	public static int I_IDQUERY = 0;
	public static int B_SUMVALUES = 1;

	private boolean sumMode;
	private int id;
	private long count;
	private boolean sumModeEmitted;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_IDQUERY, "I_IDQUERY", null, true);
		conf.registerParameter(B_SUMVALUES, "B_SUMVALUES", false, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		sumMode = getParamBoolean(B_SUMVALUES);
		id = getParamInt(I_IDQUERY);
		count = 0;
		sumModeEmitted = false;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (!sumMode) {
			// Ask the iterator to do an estimation
			TripleIterator itr = (TripleIterator) context.getInputIterator();
			long card = itr.estimateRecords();
			actionOutput.output(new TInt(id), new TLong(card));
			itr.stopReading();
			sumModeEmitted = true;
		} else {
			count += ((TLong) tuple.get(1)).getValue();
			if (count > Integer.MAX_VALUE) {
				count = Integer.MAX_VALUE;
			}
		}

	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (!sumModeEmitted && !sumMode) {
			actionOutput.output(new TInt(id), new TLong(0));
		}
		
		if (sumMode) {
			context.putObjectInCache("estimate-" + id, count);
			Lock lock = (Lock) context.getObjectFromCache(LOCK);
			lock.increase();
		}

	}
}
