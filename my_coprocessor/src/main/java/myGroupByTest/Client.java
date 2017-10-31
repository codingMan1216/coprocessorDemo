package myGroupByTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

import myGroupBy2.WxsGroupByProto3.ExpandAggregationRequest;
import myGroupBy2.WxsGroupByProto3.ExpandAggregationResponse;
import myGroupBy2.WxsGroupByProto3.ExpandCell;
import myGroupBy2.WxsGroupByProto3.RpcResultRow;
import myGroupBy2.WxsGroupByProto3.WxsGroupByService;

public class Client {
	public ExpandAggregationRequest getRequest(ArrayList<ExpandCell> groupByList, ArrayList<ExpandCell> countList,Scan scan) {
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
		try {
			requestBuilder.setScan(ProtobufUtil.toScan(scan));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return requestBuilder.build();
	}
	public HashMap<String, Integer> getGroupAndCount(Table table, ArrayList<ExpandCell> groupByList,
			ArrayList<ExpandCell> countList,Scan scan) throws ServiceException, Throwable{
		final ExpandAggregationRequest request = getRequest(groupByList, countList,scan);
		Batch.Call<WxsGroupByService, List<RpcResultRow>> callAble= new Batch.Call<WxsGroupByService, List<RpcResultRow>>(){

				public List<RpcResultRow> call(WxsGroupByService instance) throws IOException {
					// TODO Auto-generated method stub
					ServerRpcController controller = new ServerRpcController();
					BlockingRpcCallback<ExpandAggregationResponse> rpcCallback = new BlockingRpcCallback<ExpandAggregationResponse>();
					instance.getGroupAndCount(controller, request, rpcCallback);
					ExpandAggregationResponse response = rpcCallback.get();
					return response.getResultsList();
				}
				
			};
		Map<byte[], List<RpcResultRow>> result=table.coprocessorService(WxsGroupByService.class, null, null, callAble);
		HashMap<String, Integer> resultMap=new HashMap<String, Integer>();
		for (List<RpcResultRow> rpcResultRows : result.values()) {
			for (RpcResultRow rpcResultRow : rpcResultRows) {
				if (!resultMap.containsKey(rpcResultRow.getKeycolumn())) {
					resultMap.put(rpcResultRow.getKeycolumn(), rpcResultRow.getValueCount());
				}else{
					int valueCount=resultMap.get(rpcResultRow.getKeycolumn())+rpcResultRow.getValueCount();
					resultMap.put(rpcResultRow.getKeycolumn(), valueCount);
				}
			}
		}
		return resultMap;
		
	}
	public void displayResult(HashMap<String, Integer> resultmap){
		for (Map.Entry<String, Integer> entry: resultmap.entrySet()) {
			System.out.println("column:"+entry.getKey()+"\t"+"count="+entry.getValue());
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("--start--client");
		long startTime=System.currentTimeMillis();
		Connection connection = null;
		long time1=0;
		long coproCost=0;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.31.101,172.16.31.102,172.16.31.103");
		Table table=null;
		Scan scan=new Scan();
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			TableName tableName = TableName.valueOf("groupTest_1000");
			table=connection.getTable(tableName);
			 time1=System.currentTimeMillis();
			long connectionTime=time1-startTime;
			System.out.println("!!--connection创建连接的时间："+connectionTime+"毫秒");
			scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("sex"));
			scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("num"));
			ArrayList<ExpandCell> groupByList=new ArrayList<ExpandCell>();
			ExpandCell.Builder gBuilder=ExpandCell.newBuilder();
			gBuilder.setFamily("cf");
			gBuilder.setQualify("sex");
			groupByList.add(gBuilder.build());
			ArrayList<ExpandCell> countList=new ArrayList<ExpandCell>();
			ExpandCell.Builder cBuilder=ExpandCell.newBuilder();
			cBuilder.setFamily("cf");
			cBuilder.setQualify("id");
			countList.add(cBuilder.build());
			Client client=new Client();
			try {
				long coproStart=System.currentTimeMillis();
				HashMap<String, Integer> resultmap=client.getGroupAndCount(table, groupByList, countList,scan);
				long coproStop=System.currentTimeMillis();
				coproCost=coproStop-coproStart;
				client.displayResult(resultmap);
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
		long costTime=coproCost;
		System.out.println("协处理计算时间："+costTime+"毫秒");
	
	}

}
