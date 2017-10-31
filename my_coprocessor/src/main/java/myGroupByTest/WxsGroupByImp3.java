package myGroupByTest;
import java.io.FileWriter;
/**
 * group by终极版：不会先将结果放到result row的list中，而是直接累加各个结果值。未完成。
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.mapreduce.HLogInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.thrift.generated.Hbase.AsyncProcessor.majorCompact;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import myGroupBy2.WxsGroupByProto3;
import myGroupBy2.WxsGroupByProto3.RpcResultRow;
import myGroupBy2.WxsGroupByProto3.ExpandAggregationRequest;
import myGroupBy2.WxsGroupByProto3.ExpandAggregationResponse;
import myGroupBy2.WxsGroupByProto3.ExpandCell;
import myGroupBy2.WxsGroupByProto3.WxsGroupByService;
import myGroupBy2.WxsGroupByProto3.ExpandAggregationResponse.Builder;

public class WxsGroupByImp3 extends WxsGroupByService implements Coprocessor, CoprocessorService {
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
		HashSet<String> set = new HashSet<String>();
		long start=System.currentTimeMillis();
		try {
			// 通过protobuf传来的Scan,scan已经添加groupBy column和count lie
			Scan scan = ProtobufUtil.toScan(request.getScan());
			scanner = environment.getRegion().getScanner(scan);
			List<ExpandCell> groupList = request.getGroupColumnsList();
			List<ExpandCell> countList = request.getCountColumnsList();
			boolean hashNext = false;
			List<Cell> results = new ArrayList<Cell>();
			HashSet<String> hashSet=new HashSet<String>();
			HashMap<String, Integer> tmpMap=new HashMap<String, Integer>();
			ArrayList<ResultRow> resultRows=new ArrayList<ResultRow>();
			TempRow tempRow = new TempRow();
			int count=0;
			do {
				hashNext = scanner.next(results);
		
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell gCell : groupList) {
						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							tempRow.setKey(value);
							
						}
					}
					//修改一
					for (ExpandCell gCell : countList) {
						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							tempRow.setValue(value);
						}
					}
				}

//				for (Cell cell : results) {
//					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
//							cell.getFamilyLength());
//					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
//							cell.getQualifierLength());
//					for (ExpandCell gCell : countList) {
//						if (family.equals(gCell.getFamily()) && qualify.equals(gCell.getQualify())) {
//							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
//									cell.getValueLength());
//							tempRow.setValue(value);
//						}
//					}
//				}
				String key=tempRow.getKey();
				String value=tempRow.getValue();
				
				if (!set.contains(key)) {
					set.add(key);
					tmpMap.put(key, 1);
				} else {
					int newValue=tmpMap.get(key)+1;
					tmpMap.put(key, newValue);
				}
				count ++;
			} while (hashNext);
			ArrayList<RpcResultRow> rpcResultRows = changeToRpcResultRow(tmpMap);
			WxsGroupByProto3.ExpandAggregationResponse.Builder responsBuilder = WxsGroupByProto3.ExpandAggregationResponse
					.newBuilder();
			for (RpcResultRow rpcResultRow : rpcResultRows) {
				responsBuilder.addResults(rpcResultRow);
			}
			done.run(responsBuilder.build());
			long stop=System.currentTimeMillis();
			long cost=stop-start;
			writeTime(cost,count);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

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
			rpcRowBuilder.setValueCount(resultRow.getValueCount()); //imp3 修改
			rpcResultRows.add(rpcRowBuilder.build());
		}
		return rpcResultRows;
	}
	public ArrayList<RpcResultRow> changeToRpcResultRow(HashMap<String, Integer> tempMap){
		ArrayList<RpcResultRow> rpcResultRows = new ArrayList<RpcResultRow>();
		for (Map.Entry<String, Integer> entry: tempMap.entrySet()) {
			RpcResultRow.Builder rpcRowBuilder = RpcResultRow.newBuilder();
			rpcRowBuilder.setKeycolumn(entry.getKey());
			rpcRowBuilder.setValueCount(entry.getValue());
			rpcResultRows.add(rpcRowBuilder.build());
		}
		return rpcResultRows;
	}
	public void writeTime(long cosTtime,int count){
		try {
			String fileName="/home/hadoop/wxs1017/time";
			 FileWriter writer = new FileWriter(fileName, true);
	            writer.write("扫描"+count+"条，所用时间："+Long.toString(cosTtime)+"毫秒"+"\n");;
	            writer.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}

class ResultRow {
	String keyColumn = null;
	int valueCount=0;
	public ResultRow() {
	}
	public String getKeyColumn() {
		return keyColumn;
	}
	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}
	public int getValueCount() {
		return valueCount;
	}
	public void setValueCount(int valueCount) {
		this.valueCount = valueCount;
	}
	public int addValueCount(){
		this.valueCount++;
		return valueCount;
	}
}

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