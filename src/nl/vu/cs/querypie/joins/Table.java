package nl.vu.cs.querypie.joins;

import java.util.Arrays;

public class Table {
	private final int sizeRow;

	private long[] rows;
	private int idxRows;

	public Table(int sizeRow) {
		this.sizeRow = sizeRow;
		rows = new long[sizeRow];
		idxRows = 0;
	}

	public void addRow(long[] row) {
		if (idxRows == rows.length) {
			rows = Arrays.copyOf(rows, rows.length * 2);
		}
		for (int i = 0; i < sizeRow; ++i) {
			rows[idxRows++] = row[i];
		}
	}

	public int size() {
		return idxRows / sizeRow;
	}

	public int sizeRow() {
		return sizeRow;
	}
	
	public long get(int i) {
		return rows[i];
	}
}
