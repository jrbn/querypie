package arch.actions;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.files.MultiFilesReader;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class InputSplitter extends Action {

	public static final String MINIMUM_SPLIT_SIZE = "splitinput.minimumsize";
	public static final int MINIMUM_FILE_SPLIT = 4 * 1024 * 1024; // 1 MB

	static final Logger log = LoggerFactory.getLogger(InputSplitter.class);
	
	Tuple tuple = new Tuple();

	private Chain addNewChain(ActionContext context, Chain inputChain,
			int begin, int end, TString path, TString filter) throws Exception {

		Chain newChain = new Chain();
		inputChain.generateChild(context, newChain);

		String newInput = path.getValue() + ":" + begin + ":" + end;
		log.debug("Add new chain " + newInput);
		if (filter != null)
			tuple.set(new TString(newInput), filter);
		else
			tuple.set(new TString(newInput));

		newChain.setInputTuple(tuple);
		newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);
		return newChain;
	}

	@Override
	public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
			WritableContainer<Chain> outputChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
		inputChain.addAction(InputSplitter.class.getName(), null, (Object[])null);
		return inputChain;
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception {

		TString path = new TString();
		TString filter = null;

		// Tuple is a directory. Splits the chain in many others depending on
		// the size of the input
		inputTuple.get(path, 0);
		if (inputTuple.getNElements() > 1) {
			filter = new TString();
			inputTuple.get(filter, 1);
		}

		int minimumFileSplit = context.getConfiguration().getInt(
				MINIMUM_SPLIT_SIZE, MINIMUM_FILE_SPLIT);

		File file = new File(path.getValue());
		if (file.isDirectory()) {
			// Split it in small chunks

			List<File> allfiles;
			if (filter == null) {
				allfiles = MultiFilesReader.listAllFiles(path.getValue(), null);
			} else {
				allfiles = MultiFilesReader.listAllFiles(path.getValue(),
						filter.getValue());
			}

			int sizeChunk = 0;
			int beginChunkIndex = 0;
			int endChunkIndex = 0;

			for (File child : allfiles) {
				endChunkIndex++;
				sizeChunk += child.length();
				if (sizeChunk > minimumFileSplit) {
					Chain newChain = addNewChain(context, remainingChain,
							beginChunkIndex, endChunkIndex, path, filter);
					chainsToProcess.add(newChain);
					beginChunkIndex = endChunkIndex;
					sizeChunk = 0;
				}
			}

			// Copy the remaining
			if (beginChunkIndex < allfiles.size()) {
				Chain newChain = addNewChain(context, remainingChain,
						beginChunkIndex, allfiles.size(), path, filter);
				chainsToProcess.add(newChain);
			}

		} else {
			// TODO: we can split only directories
		}
	}
}
