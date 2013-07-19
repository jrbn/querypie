package arch.actions;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class WriteToFile extends Action {

	final static Logger log = LoggerFactory.getLogger(WriteToFile.class);

	static public class StandardFileWriter {

		FileWriter writer = null;
		DataProvider dp = null;
		SimpleData[] array = null;

		public StandardFileWriter(ActionContext context, File file)
				throws IOException {
			writer = new FileWriter(file);
			dp = context.getDataProvider();
		}

		public StandardFileWriter() {
		}

		public void write(Tuple tuple) throws Exception {
			if (array == null) {
				array = new SimpleData[tuple.getNElements()];
				for (int i = 0; i < tuple.getNElements(); ++i) {
					array[i] = dp.get(tuple.getType(i));
				}
			}

			tuple.get(array);
			String value = "";
			for (SimpleData v : array) {
				value += v.toString() + "\t";
			}
			writer.write(value + "\n");
		}

		public void close() throws IOException {
			writer.close();
		}
	}

	public static final String FILE_IMPL_WRITER = "writetoFile.impl";

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain chain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToSend) throws Exception {
		TString file = new TString();
		tuple.get(file, 0);
		chain.addAction(this, null, file.getValue());
		return chain;
	}

	StandardFileWriter file = null;

	@Override
	public void startProcess(ActionContext context, Chain chain,
			Object... params) throws Exception {
		String fileName = (String) params[0];
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);

		// Calculate the filename
		File f = new File(fileName);

		if (!f.exists()) {
			f.mkdir();
		}

		f = new File(f, "part-"
				+ nf.format(context.getNetworkLayer().getMyPartition()) + "_"
				+ nf.format(0));

		String writerImplementation = context.getConfiguration().get(
				FILE_IMPL_WRITER, null);
		try {
			if (writerImplementation != null) {
				Constructor<? extends StandardFileWriter> constr = Class
						.forName(writerImplementation)
						.asSubclass(StandardFileWriter.class)
						.getConstructor(ActionContext.class, File.class);
				file = constr.newInstance(context, f);
			} else {
				log.info("No custom writer is specified. Using standard one");
				file = new StandardFileWriter(context, f);
			}
		} catch (Exception e) {
			log.error("Error instantiating writer for file " + file + "("
					+ writerImplementation + ")", e);
			file = null;
		}
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		if (file != null)
			file.write(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> newChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
		if (file != null) {
			file.close();
		}
		file = null;
	}

}
