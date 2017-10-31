package groupByDemo;

import java.util.ArrayList;
import java.util.List;

public class ExpandHbaseCell {
	private String family;

	private String qualify;

	private String value;

	private List<String> values = new ArrayList<String>();

	private String className;

	public ExpandHbaseCell() {

	}

	public ExpandHbaseCell(String family, String qualify, String className) {
		this.family = family;
		this.qualify = qualify;
		this.className = className;
	}

	public ExpandHbaseCell(String family, String qualify) {
		this.family = family;
		this.qualify = qualify;
	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getQualify() {
		return qualify;
	}

	public void setQualify(String qualify) {
		this.qualify = qualify;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}
}
