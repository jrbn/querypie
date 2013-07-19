package arch.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.storage.Container;
import arch.storage.Writable;

class ChainTerminator implements Runnable {

    public static class ChainInfo extends Writable {

	public int nodeId;
	public int submissionId;
	public long chainId;
	public long parentChainId;
	public int repFactor;
	public int nchildrens;

	public ChainInfo() {

	}

	@Override
	public void readFrom(DataInput input) throws IOException {
	    nodeId = input.readInt();
	    submissionId = input.readInt();
	    chainId = input.readLong();
	    parentChainId = input.readLong();
	    repFactor = input.readInt();
	    nchildrens = input.readInt();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
	    output.writeInt(nodeId);
	    output.writeInt(submissionId);
	    output.writeLong(chainId);
	    output.writeLong(parentChainId);
	    output.writeInt(repFactor);
	    output.writeInt(nchildrens);
	}

	@Override
	public int bytesToStore() {
	    return 32;
	}

    }

    static final Logger log = LoggerFactory.getLogger(ChainTerminator.class);

    Context context;
    Container<ChainInfo> chainsTerminated;

    public ChainTerminator(Context context,
	    Container<ChainInfo> chainsTerminated) {
	this.context = context;
	this.chainsTerminated = chainsTerminated;
    }

    @Override
    public void run() {
	ChainInfo header = new ChainInfo();
	while (true) {
	    try {
		chainsTerminated.remove(header);

		NetworkLayer ibis = context.getNetworkLayer();
		IbisIdentifier identifier = ibis.getPeerLocation(header.nodeId);
		WriteMessage msg = ibis.getMessageToSend(identifier,
			NetworkLayer.nameMgmtReceiverPort);
		msg.writeByte((byte) 2);
		msg.writeInt(header.submissionId);
		msg.writeLong(header.chainId);
		msg.writeLong(header.parentChainId);
		msg.writeInt(header.repFactor);
		msg.writeInt(header.nchildrens);
		ibis.finishMessage(msg, header.submissionId);
		if (log.isDebugEnabled()) {
		    log.debug("Sent message with id 2 to " + identifier);
		}

	    } catch (Exception e) {
		log.error("Error in sending the termination codes", e);
	    }

	}
    }

}
