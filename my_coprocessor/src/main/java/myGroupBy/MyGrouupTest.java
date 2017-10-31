package myGroupBy;

/**
 * 2017.09.28
 * 使用scanner代替internalScanner实现协处理器服端逻辑。
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.kenai.jffi.Array;

public class MyGrouupTest {
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.31.101,172.16.31.102,172.16.31.103");
		Connection connection = null;
		List<ExpandCell> groupbyList = new ArrayList<ExpandCell>();
		List<ExpandCell> countList = new ArrayList<ExpandCell>();
		ExpandCell group = new ExpandCell("cf", "id");
		ExpandCell count = new ExpandCell("cf", "id");
		groupbyList.add(group);
		countList.add(count);
		List<String> gkeys = new ArrayList<String>();

		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Table talbe = null;
		ResultScanner rs = null;
		ArrayList<TempRow> rows = new ArrayList<TempRow>();
		try {
			talbe = connection.getTable(TableName.valueOf("wxs0927"));
			Scan scan = new Scan();
			rs = talbe.getScanner(scan);
			for (Result result : rs) {// 所有行
				TempRow tempRow = new TempRow();
				for (Cell cell : result.listCells()) {// 一行
					// String family=new String(CellUtil.cloneFamily(cell));
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					// System.out.println(family);
					// System.err.println(qualify);
					for (ExpandCell gCell : groupbyList) {
						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							System.out.println(value);
							tempRow.setKey(value);
						}
					}
				}

				for (Cell cell : result.listCells()) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell gCell : countList) {
						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							System.out.println(value);
							tempRow.setValue(value);
						}
					}
				}
				rows.add(tempRow);
			}
			System.out.println("--------" + rows.size());
			MyGrouupTest mgt = new MyGrouupTest();
			mgt.analyzeRow(rows);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (Exception e2) {
				// TODO: handle exception
				e2.printStackTrace();
			}
		}
	}

	public void analyzeRow(ArrayList<TempRow> rows) {
		Iterator<TempRow> iterator = rows.iterator();
		ArrayList<ResultRow> resultRows = new ArrayList<ResultRow>();
		HashSet<String> set = new HashSet<String>();
		//int i = 0;
		while (iterator.hasNext()) {
			TempRow tempRow = iterator.next();
			String key = tempRow.getKey();
			if (!set.contains(key)) {
				set.add(key);
				ResultRow resultRow = new ResultRow();
				resultRow.setKeyColumn(key);
				resultRow.getColumnValue().add(tempRow.getValue());
				resultRows.add(resultRow);
			} else {

				ResultRow resultRowtmp = getResultRow(key, resultRows);
				resultRowtmp.getColumnValue().add(tempRow.getValue());
				// getResultRow(key,
				// resultRows).getColumnValue().add(tempRow.getValue());
			}
			//i++;
		}
		displayResult(resultRows);

	}

	public void displayResult(ArrayList<ResultRow> resultRows) {
		for (ResultRow resultRow : resultRows) {
			System.out.println("column:" + resultRow.getKeyColumn() + "       count=" + resultRow.getColumnValue().size());
		}
	}
	//根据keyName，从resultRows中获取对应的resultRow
	public ResultRow getResultRow(String keyName, ArrayList<ResultRow> resultRows) {
		for (ResultRow resultRow : resultRows) {
			if (keyName.equals(resultRow.getKeyColumn())) {
				return resultRow;
			}
		}
		return null;

	}
}

class ResultRow {
	String keyColumn = null;
	List<String> columnValue = null;

	public ResultRow() {
		// TODO Auto-generated constructor stub
		columnValue = new ArrayList<String>();
	}

	public String getKeyColumn() {
		return keyColumn;
	}

	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}

	public List<String> getColumnValue() {
		return columnValue;
	}

	public void setColumnValue(List<String> columnValue) {
		this.columnValue = columnValue;
	}

}

// 临时行
class TempRow {
	private String key = null;
	private String value = null;

	public TempRow() {

	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}

class ExpandCell {
	private String family = null;
	private String qualify = null;
	private String value = null;

	public ExpandCell(String family, String qualify) {
		// TODO Auto-generated constructor stub
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

}
