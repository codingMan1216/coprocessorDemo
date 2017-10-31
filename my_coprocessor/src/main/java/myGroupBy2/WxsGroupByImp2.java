package myGroupBy2;
/**
 * group by终极版：不会先将结果放到result row的list中，而是直接累加各个结果值。未完成。
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import myGroupBy2.WxsGroupByProto3.ExpandCell;
import myGroupBy2.WxsGroupByProto3.ExpandAggregationRequest;
import myGroupBy2.WxsGroupByProto3.ExpandAggregationResponse;
import myGroupBy2.WxsGroupByProto3.RpcResultRow;
import myGroupBy2.WxsGroupByProto3.WxsGroupByService;
//import myGroupByTest.ResultRow;
//import myGroupByTest.TempRow;

public class WxsGroupByImp2 extends WxsGroupByService implements Coprocessor, CoprocessorService {
	private RegionCoprocessorEnvironment environment;

	public Service getService() {
		// TODO Auto-generated method stub
		return this;
	}

	public void start(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
		if (env instanceof RegionCoprocessorEnvironment) {
			this.environment = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getGroupAndCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub
		InternalScanner scanner = null;
		try {
			// 通过protobuf传来的Scan,scan已经添加groupBy column和count lie
			Scan scan = ProtobufUtil.toScan(request.getScan());
			scanner = environment.getRegion().getScanner(scan);
			List<ExpandCell> groupList = request.getGroupColumnsList();
			List<ExpandCell> countList = request.getCountColumnsList();
			boolean hashNext = false;
			List<Cell> results = new ArrayList<Cell>();
			ArrayList<TempRow> rows = new ArrayList<TempRow>();
			do {
				hashNext = scanner.next(results);//id 4
				TempRow tempRow = new TempRow();
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell gCell : groupList) {
						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							System.out.println(value);
							tempRow.setKey(value);
						}
					}
				}

				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell gCell : countList) {
						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							tempRow.setValue(value);
							
						}
					}
				}
				rows.add(tempRow);
			} while (hashNext);
			ArrayList<ResultRow> ResultRows = analyzeRow(rows);
			ArrayList<RpcResultRow> rpcResultRows = changeToRpcResultRow(ResultRows);
			WxsGroupByProto3.ExpandAggregationResponse.Builder responsBuilder = WxsGroupByProto3.ExpandAggregationResponse
					.newBuilder();
			for (RpcResultRow rpcResultRow : rpcResultRows) {
				responsBuilder.addResults(rpcResultRow);
			}
			done.run(responsBuilder.build());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public ArrayList<ResultRow> analyzeRow(ArrayList<TempRow> rows) {
		Iterator<TempRow> iterator = rows.iterator();
		ArrayList<ResultRow> resultRows = new ArrayList<ResultRow>();
		HashSet<String> set = new HashSet<String>();
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
			}
		}
		return resultRows;

	}

	// 根据keyName，从resultRows中获取对应的resultRow
	public ResultRow getResultRow(String keyName, ArrayList<ResultRow> resultRows) {
		for (ResultRow resultRow : resultRows) {
			if (keyName.equals(resultRow.getKeyColumn())) {
				return resultRow;
			}
		}
		return null;

	}

	// 将analyzeRow()方法返回的resultRow集合转化成能够用于RPC通讯使用的RpcResultRow集合。
	public ArrayList<RpcResultRow> changeToRpcResultRow(ArrayList<ResultRow> resultRows) {
		ArrayList<RpcResultRow> rpcResultRows = new ArrayList<RpcResultRow>();
		
		Iterator<ResultRow> iterator = resultRows.iterator();
		ResultRow resultRow = null;
		while (iterator.hasNext()) {
			resultRow = iterator.next();
			RpcResultRow.Builder rpcRowBuilder = RpcResultRow.newBuilder();
			rpcRowBuilder.setKeycolumn(resultRow.getKeyColumn());
			rpcRowBuilder.setValueCount(resultRow.getColumnValue().size());
			rpcResultRows.add(rpcRowBuilder.build());
		}
		return rpcResultRows;
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