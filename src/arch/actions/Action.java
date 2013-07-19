package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.Writable;
import arch.storage.container.WritableContainer;

public abstract class Action extends Writable {

    public boolean blockProcessing() {
	return false;
    }

    @Override
    public void readFrom(DataInput input) throws IOException {
    }

    @Override
    public void writeTo(DataOutput output) throws IOException {
    }

    @Override
    public int bytesToStore() {
	return 0;
    }

    /******
     * Method called after the action is instantiated
     * 
     * @param chainsToSend
     *            TODO
     ******/
    // public void startExpansion(ActionContext context) {
    //
    // }

    public abstract Chain apply(ActionContext context, Tuple tuple,
	    Chain chain, WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception;

    /******
     * Method called during the execution of the chain
     * 
     * @throws Exception
     ******/
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
    }

    public abstract void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
	    throws Exception;

    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output, WritableContainer<Chain> newChains, WritableContainer<Chain> chainsToSend)
	    throws Exception {
    }

}