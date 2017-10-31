package groupByDemo;

import java.util.List;

public class ExpandHbaseRow {
	private List<ExpandHbaseCell> keys;
	private List<ExpandHbaseCell> resultRow;

	public int size() {
		if (this.resultRow == null)
			return 0;
		return this.resultRow.size();
	}

	public List<ExpandHbaseCell> getResultRow() {
		return resultRow;
	}

	public void setResultRow(List<ExpandHbaseCell> resultRow) {
		this.resultRow = resultRow;
	}

	public List<ExpandHbaseCell> getKeys() {
		return keys;
	}

	public void setKeys(List<ExpandHbaseCell> keys) {
		this.keys = keys;
	}
}
