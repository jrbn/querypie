package nl.vu.cs.querypie.indices;

import java.io.File;
import java.io.IOException;

import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.disk.PlainTripleFile;

import arch.ActionContext;
import arch.actions.WriteToFile;
import arch.data.types.Tuple;

public class CompressedWriter extends WriteToFile.StandardFileWriter {

	public CompressedWriter(ActionContext context, File file)
			throws IOException {
		super();
		this.file = new PlainTripleFile(file.getAbsolutePath());
		this.file.openToWrite();
	}

	PlainTripleFile file = null;
	RDFTerm s = new RDFTerm();
	RDFTerm p = new RDFTerm();
	RDFTerm o = new RDFTerm();

	@Override
	public void write(Tuple tuple) throws Exception {
		tuple.get(s, p, o);
		file.write(s.getValue(), p.getValue(), o.getValue());
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

}
