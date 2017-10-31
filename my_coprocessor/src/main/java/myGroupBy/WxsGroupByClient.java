package myGroupBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.google.protobuf.ServiceException;

//import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationResponse;
import myGroupBy.WxsGroupByProto.ExpandAggregationRequest;
import myGroupBy.WxsGroupByProto.ExpandAggregationResponse;
import myGroupBy.WxsGroupByProto.ExpandCell;
import myGroupBy.WxsGroupByProto.RpcResultRow;
import myGroupBy.WxsGroupByProto.WxsGroupByService;
//import myGroupByTest.ResultRow;

public class WxsGroupByClient {
	// 生成reguest请求的方法
	public ExpandAggregationRequest getRequest(ArrayList<ExpandCell> groupByList, ArrayList<ExpandCell> countList) {
		ExpandAggregationRequest.Builder requestBuilder = ExpandAggregationRequest.newBuilder();
		for (ExpandCell groupByColumn : groupByList) {
			if (groupByColumn != null) {
				requestBuilder.addGroupColumns(groupByColumn);
			}
		}
		for (ExpandCell countColumn : countList) {
			if (countColumn != null) {
				requestBuilder.addCountColumns(countColumn);
			}
		}
		return requestBuilder.build();
	}

	public ArrayList<ResultRow> getGroupByAndCount(Table table, ArrayList<ExpandCell> groupByList,
			ArrayList<ExpandCell> countList) throws ServiceException, Throwable {
		final ExpandAggregationRequest request = getRequest(groupByList, countList);

		class GetGroupByAndCountCallBack implements Batch.Callback<List<RpcResultRow>> {
			ArrayList<ResultRow> resultRows = new ArrayList<ResultRow>();

			public ResultRow changeRpcResultRow(RpcResultRow rpcResultRow) {
				ResultRow resultRow = new ResultRow();
				resultRow.setKeyColumn(rpcResultRow.getKeycolumn());
				List<String> columnValueList = rpcResultRow.getColumnValueList();
				resultRow.setColumnValue(columnValueList);
				return resultRow;
			}

			public ArrayList<ResultRow> getGroupAndCount() {
				return resultRows;
			}

			public void update(byte[] region, byte[] row, List<RpcResultRow> results) {
				for (RpcResultRow rpcResultRow : results) {
					ResultRow resultRow = changeRpcResultRow(rpcResultRow);
					resultRows.add(resultRow);
				}

			}
			
		}
		GetGroupByAndCountCallBack getGroupByAndCountCallBack = new GetGroupByAndCountCallBack();
		
		table.coprocessorService(WxsGroupByService.class, null, null,
				new Batch.Call<WxsGroupByService, List<RpcResultRow>>() {

					public List<RpcResultRow> call(WxsGroupByService instance) throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<ExpandAggregationResponse> rpcCallback = new BlockingRpcCallback<ExpandAggregationResponse>();
						instance.getGroupAndCount(controller, request, rpcCallback);
						ExpandAggregationResponse response = rpcCallback.get();

						return response.getResultsList();
					}
				}, getGroupByAndCountCallBack);
		return getGroupByAndCountCallBack.getGroupAndCount();

	}
	public void displayResult(ArrayList<ResultRow> resultRows) {
		for (ResultRow resultRow : resultRows) {
			System.out.println("column:" + resultRow.getKeyColumn() + "       count=" + resultRow.getColumnValue().size());
		}
	}
	public static void main(String[] args) {
		System.out.println("--start--");
		long startTime=System.currentTimeMillis();
		Connection connection = null;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.31.101,172.16.31.102,172.16.31.103");
		Table table=null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			TableName tableName = TableName.valueOf("wxs0927");
			table=connection.getTable(tableName);
			ArrayList<ExpandCell> groupByList=new ArrayList<WxsGroupByProto.ExpandCell>();
			ExpandCell.Builder gBuilder=ExpandCell.newBuilder();
			gBuilder.setFamily("cf");
			gBuilder.setQualify("sex");
			groupByList.add(gBuilder.build());
			ArrayList<ExpandCell> countList=new ArrayList<WxsGroupByProto.ExpandCell>();
			ExpandCell.Builder cBuilder=ExpandCell.newBuilder();
			cBuilder.setFamily("cf");
			cBuilder.setQualify("num");
			countList.add(cBuilder.build());
			WxsGroupByClient wxsc=new WxsGroupByClient();
			try {
				ArrayList<ResultRow> resultRows=wxsc.getGroupByAndCount(table, groupByList, countList);
				wxsc.displayResult(resultRows);
			} catch (ServiceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				table.close();
				connection.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		long stopTime=System.currentTimeMillis();
		long costTime=stopTime-startTime;
		System.out.println("总耗时："+costTime/1000+"秒");
	}
}
