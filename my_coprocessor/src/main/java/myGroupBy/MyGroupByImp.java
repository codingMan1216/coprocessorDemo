package myGroupBy;
/**
 * date：2017.10.10
 * author：wxs
 * 协处理器服务端实现
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import myGroupBy.MyGroupByProto.ExpandCell;
import myGroupBy.MyGroupByProto.MyGroupByRequest;
import myGroupBy.MyGroupByProto.MyGroupByResponse;
import myGroupBy.MyGroupByProto.MyGroupByService;

public class MyGroupByImp extends MyGroupByService implements Coprocessor, CoprocessorService {
	private RegionCoprocessorEnvironment environment;

	public Service getService() {
		// TODO Auto-generated method stub
		return null;
	}

	public void start(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getGroupAndSum(RpcController controller, MyGroupByRequest request,
			RpcCallback<MyGroupByResponse> done) {
		InternalScanner internalScanner = null;
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());// 客户端通过protobuf传输过来的scan
			internalScanner = environment.getRegion().getScanner(scan);// region内部scanner
			List<ExpandCell> groupList = request.getGroupColumsList();// 要group
																		// by的列
			List<ExpandCell> sumlist = request.getSumColumsList();// 要sum的列 sex
			HashSet<String> groupSet=new HashSet<String>();
			List<Cell> results = new ArrayList<Cell>();
			boolean hasNext = false;
			hasNext = internalScanner.next(results);
			while (hasNext) {
				for (Cell cell : results) {
					//String family=new String(CellUtil.cloneFamily(cell));//两种实现获取cf和column方式
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					//String qualify=new String(CellUtil.cloneQualifier(cell));
					String qualify = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell expandCell : groupList) {
						if (family.equals(expandCell.getFamily().toString())&&qualify.equals(expandCell.getFamily().toString())) {
							String value=Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset());
							
							
							
						}
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
class TmpRow{//临时row，属性key是group by列的值，results就是scannner扫描出来的那一行。
	private String key;
	private List<Cell> results;
	public TmpRow() {
		// TODO Auto-generated constructor stub
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public List<Cell> getResults() {
		return results;
	}
	public void setResults(List<Cell> results) {
		this.results = results;
	}
	
}