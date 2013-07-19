package nl.vu.cs.querypie.indices;

import java.io.File;
import java.io.FilenameFilter;

public class CompressedFilesFilter implements FilenameFilter {

	@Override
	public boolean accept(File dir, String name) {
		if (name.startsWith("dir-")
				|| (!name.startsWith(".") && !name.startsWith("_")))
			return true;
		return false;
	}
}
