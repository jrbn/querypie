package arch.datalayer.files;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.chains.Chain;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;

public class FilesLayer extends InputLayer {

    public final static String IMPL_FILE_READER = "fileslayer.reader.impl";

    static final Logger log = LoggerFactory.getLogger(FilesLayer.class);

    Factory<TString> factory = new Factory<TString>(TString.class);
    int numberNodes;
    int currentPivot;

    Class<? extends FileIterator> classFileIterator = null;

    @Override
    protected void load(Context context) throws IOException {
	currentPivot = -1;
	numberNodes = context.getNetworkLayer().getNumberNodes();

	String clazz = context.getConfiguration().get(IMPL_FILE_READER, null);
	try {
			classFileIterator = Class.forName(clazz).asSubclass(
		    FileIterator.class);
	} catch (Exception e) {
	    log.error("Failed in loading the file reader class", e);
	}

    }

    @Override
    public TupleIterator getIterator(Tuple tuple, ActionContext context) {
	try {
	    TString value = factory.get();
	    tuple.get(value, 0);
	    String sFilter = null;
	    if (tuple.getNElements() > 1) {
		TString filter = factory.get();
		tuple.get(filter, 1);
		sFilter = filter.getValue();
		factory.release(filter);
	    }

	    TupleIterator itr = new MultiFilesReader(value.getValue(), sFilter,
		    classFileIterator);
	    factory.release(value);
	    return itr;
	} catch (Exception e) {
	    log.error("Unable getting tuple iterator", e);
	}

	return null;
    }

    @Override
    public int[] getLocations(Tuple tuple, Chain chain, Context context) {
	int[] range = new int[2];
	if (++currentPivot == numberNodes) {
	    currentPivot = 0;
	}

	range[0] = range[1] = currentPivot;
	return range;
    }

    @Override
    public void releaseIterator(TupleIterator itr, ActionContext context) {
    }

}
