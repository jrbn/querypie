package nl.vu.cs.querypie.sparql;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.joins.Table;
import nl.vu.cs.querypie.storage.RDFTerm;

public class AddToTable extends Action {
	
	public static final int S_NAMETABLE = 0;
	public static final int I_SIZEROW = 1;
	
	private Table table;
	private int sizeRow;
	private long[] internalRow;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_NAMETABLE, "S_NAMETABLE", null, true);
		conf.registerParameter(I_SIZEROW, "I_SIZEROW", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		sizeRow = getParamInt(I_SIZEROW);
		table = new Table(sizeRow);
		internalRow = new long[sizeRow];
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		for(int i = 0; i < sizeRow; ++i) {
			internalRow[i] = ((RDFTerm)tuple.get(i)).getValue();
		}
		table.addRow(internalRow);
	}
	
	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		context.putObjectInCache(getParamString(S_NAMETABLE), table);
		table = null;
		internalRow = null;
	}
}
