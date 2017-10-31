package groupByDemo;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationRequest;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationResponse;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationService;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandCell;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandRow;
public class ExpandAggregationClient {
//private Log log = LogFactory.getLog(ExpandAggregationClient.class);
    
    Configuration conf;
    
    public ExpandAggregationClient(Configuration conf){
        this.conf = conf;
    }
    
    public List<ExpandHbaseRow> getGroupSumAndCount(final String tableName,ExpandHbaseCondition condition,Scan scan) throws Throwable{
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
            return getGroupSumAndCount(table,condition,scan);
        }finally{
            table.close();
        }
    }
    
    public List<ExpandHbaseRow> getGroupSumAndCount(HTable table,ExpandHbaseCondition condition,Scan scan) throws Throwable{
        final ExpandAggregationRequest request = validateArgAndGetPB(condition, scan, true, true, true, false);
        
        class GroupSumAndCountCallback implements Batch.Callback<List<ExpandRow>>{
            List<ExpandCell> sumList = request.getSumColumnsList();
            List<ExpandCell> countList = request.getCountColumnsList();
            List<ExpandHbaseRow> list = new ArrayList<ExpandHbaseRow>();
            public List<ExpandHbaseRow> getGroupSumAndCount(){
                return list;
            }
            /**
            * @Title: update
            * @Description: 
            * @param 
            * @return Batch.Callback<List<ExpandHbaseRow>>
            * @author lpy
            * @throws
            */
            public synchronized void update(byte[] region, byte[] row, List<ExpandRow> result) {
                if(list.size() == 0){
                    if(result !=null){
                        for(ExpandRow rowN : result){
                            list.add(HbaseUtil.toExpandHbaseRow(rowN));
                        }
                    }
                }else{
                    List<ExpandHbaseRow> locat = new ArrayList<ExpandHbaseRow>();
                    if(result != null){
                        for(ExpandRow rowN : result){
                            locat.add(HbaseUtil.toExpandHbaseRow(rowN));
                        }
                    }
                    for(ExpandHbaseRow listRow :  list){
                        List<ExpandHbaseCell>  listCells = listRow.getKeys();
                        for(ExpandHbaseRow locatRow : locat){
                            List<ExpandHbaseCell> locatCells = locatRow.getKeys();
                            if(listCells.size() == locatCells.size()){
                                boolean flag = false;//假设不是同一个组
                                for(int i=0;i<listCells.size();i++){
                                    //是同一个组
                                    if(listCells.get(i).getFamily().equals(locatCells.get(i).getFamily())&&
                                            listCells.get(i).getQualify().equals(locatCells.get(i).getQualify()) &&
                                            ((listCells.get(i).getValue() == null && locatCells.get(i).getValue() == null) ||
                                                    listCells.get(i).getValue()!=null && listCells.get(i).getValue().equals(locatCells.get(i).getValue()))){
                                        flag = true;
                                        break;
                                    }
                                }
                                if(flag){
                                    List<ExpandHbaseCell> listCol = listRow.getResultRow();
                                    List<ExpandHbaseCell> locatCol =locatRow.getResultRow();
                                    for(ExpandCell sumCell:sumList){
                                        for(ExpandHbaseCell cell : listCol){
                                            boolean check = false;
                                            if(sumCell.getFamily().toStringUtf8().equals(cell.getFamily()) &&
                                                    sumCell.getQualify().toStringUtf8().equals(cell.getQualify())){
                                                for(ExpandHbaseCell lcol : locatCol){
                                                    if(lcol.getFamily().equals(cell.getFamily()) && lcol.getQualify().equals(cell.getQualify())){
                                                        String v = AddValue(cell.getValue(),lcol.getValue(),sumCell.getClassName());
                                                        if(v==null) cell.setValue("0");
                                                        else cell.setValue(v);
                                                        check = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if(check) break;
                                        }
                                    }
                                    
                                    for(ExpandCell countCell:countList){
                                        for(ExpandHbaseCell cell : listCol){
                                            boolean check = false;
                                            if(countCell.getFamily().toStringUtf8().equals(cell.getFamily()) &&
                                                    countCell.getQualify().toStringUtf8().equals(cell.getQualify())){
                                                for(ExpandHbaseCell lcol : locatCol){
                                                    if(lcol.getFamily().equals(cell.getFamily()) && lcol.getQualify().equals(cell.getQualify())){
                                                        String v = AddValue(cell.getValue(),lcol.getValue(),ByteString.copyFromUtf8("Long"));
                                                        if(v==null) cell.setValue("1");
                                                        else cell.setValue(v);
                                                        check = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if(check) break;
                                        }
                                    }
                                }else{
                                    list.add(locatRow);
                                }
                                
                            }
                        }
                    }
                    
                    locat.clear();
                }
                
            }
            
        }
        
        
        GroupSumAndCountCallback groupSumAndCountcallback = new GroupSumAndCountCallback();
        table.coprocessorService(ExpandAggregationService.class, scan.getStartRow(), scan.getStopRow(), new Batch.Call<ExpandAggregationService, List<ExpandRow>>() {
            /**
            * @Title: call
            * @Description: 
            * @param return null;
            * @return Batch.Call<ExpandAggregationService,List<ExpandRow>>
            * @author lpy
            * @throws
            */
            public List<ExpandRow> call(ExpandAggregationService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<ExpandAggregationResponse> rpcCallback 
                    = new BlockingRpcCallback<ExpandAggregationResponse>();
                instance.getGroupSumAndCount(controller, request, rpcCallback);
                ExpandAggregationResponse response = rpcCallback.get();
                 if (controller.failedOnException()) {
                      throw controller.getFailedOn();
                 }
                 return response.getResultsList();
            }

            
            
        },groupSumAndCountcallback);
        return groupSumAndCountcallback.getGroupSumAndCount();
    }
    
    
    
    public List<ExpandHbaseRow> getDistinctCount(final String tableName,ExpandHbaseCondition condition,Scan scan) throws Throwable{
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
            List<ExpandHbaseRow> list = getDistinctCount(table,condition,scan);
            if(list.size() != 0){ 
                List<ExpandHbaseCell> cells = list.get(0).getResultRow();
                for(ExpandHbaseCell cell : cells){
                    int num = cell.getValues().size();
                    cell.setValue(num+"");
                }
                return list;
            }
            return null;
                
        }finally{
            table.close();
        }
    }
    
    public List<ExpandHbaseRow> getDistinctCount(HTable table,ExpandHbaseCondition condition,Scan scan) throws ServiceException, Throwable{
        final ExpandAggregationRequest request = validateArgAndGetPB(condition, scan, false, false, false, true);
        class DistinctCount implements Batch.Callback<List<ExpandRow>>{
            List<ExpandHbaseRow> list = new ArrayList<ExpandHbaseRow>();
            public List<ExpandHbaseRow> getDistinctCount(){
                return list;
            }
            /**
            * @Title: update
            * @Description: 
            * @param 
            * @return Batch.Callback<List<ExpandRow>>
            * @author lpy
            * @throws
            */
            public void update(byte[] region, byte[] row, List<ExpandRow> result) {
                if(list.size() == 0){
                    if(result !=null){
                        for(ExpandRow rowN : result){
                            list.add(HbaseUtil.toExpandHbaseRow(rowN));
                        }
                    }
                }else{
                    List<ExpandHbaseRow> locat = new ArrayList<ExpandHbaseRow>();
                    if(result != null){
                        for(ExpandRow rowN : result){
                            locat.add(HbaseUtil.toExpandHbaseRow(rowN));
                        }
                    }
                    for(ExpandHbaseRow listRow : list){
                        List<ExpandHbaseCell> listCells = listRow.getResultRow();
                        for(ExpandHbaseRow locatRow:locat){
                            List<ExpandHbaseCell> locatCells = locatRow.getResultRow();
                            for(ExpandHbaseCell listCell:listCells){
                                for(ExpandHbaseCell locatCell:locatCells){
                                    if(listCell.getFamily().equals(locatCell.getFamily()) &&
                                            listCell.getQualify().equals(locatCell.getQualify())){
                                        List<String> vs = locatCell.getValues();
                                        for(String s:vs){
                                            if(!listCell.getValues().contains(s)) listCell.getValues().add(s);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
            }
        }
        
        DistinctCount distinctCount = new DistinctCount();
        
        table.coprocessorService(ExpandAggregationService.class, scan.getStartRow(), scan.getStopRow(), new Batch.Call<ExpandAggregationService, List<ExpandRow>>() {
            
            /**
            * @Title: call
            * @Description: 
            * @param return null;
            * @return Batch.Call<ExpandAggregationService,List<ExpandRow>>
            * @author lpy
            * @throws
            */
            public List<ExpandRow> call(ExpandAggregationService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<ExpandAggregationResponse> rpcCallback 
                    = new BlockingRpcCallback<ExpandAggregationResponse>();
                instance.getDistictCount(controller, request, rpcCallback);
                ExpandAggregationResponse response = rpcCallback.get();
                 if (controller.failedOnException()) {
                      throw controller.getFailedOn();
                 }
                 return response.getResultsList();
            }
            
        },distinctCount);
        return distinctCount.getDistinctCount();
    }
    /**
     * 把统计字段与查询条件变成一个
     * request对象
     * @param condition
     * @param scan
     * @param isSum
     * @param isCount
     * @param isGroup
     * @param isDistict
     * @return
     * @throws IOException 
     */
    public ExpandAggregationRequest validateArgAndGetPB(ExpandHbaseCondition condition,Scan scan,
            final boolean isSum,final boolean isCount,
            final boolean isGroup,final boolean isDistict) throws IOException{
       // AssertPropertiseException.notNull(condition, "统计的对象不能为空！！");
        ExpandAggregationRequest.Builder builder = ExpandAggregationRequest.newBuilder();
        builder.setScan(ProtobufUtil.toScan(scan));
        if(isSum){
            if(condition.getSumList() == null || condition.getSumList().size() == 0 ){
                throw new IllegalArgumentException("sumList不能为空！");
            }
            for(ExpandHbaseCell cell :condition.getSumList())
                builder.addSumColumns(HbaseUtil.toExpandCell(cell));
        }
        if(isCount){
            if(condition.getCountList() == null || condition.getCountList().size() == 0){
                throw new IllegalArgumentException("countList不能为空！");
            }
            for(ExpandHbaseCell cell :condition.getCountList()){
                builder.addCountColumns(HbaseUtil.toExpandCell(cell));
            }
        }
        if(isGroup){
            if(condition.getGroupList() == null || condition.getGroupList().size() == 0){
                throw new IllegalArgumentException("groupList不能为空！");
            }
            for(ExpandHbaseCell cell :condition.getGroupList()){
                builder.addGroupColumns(HbaseUtil.toExpandCell(cell));
            }
        }
        if(isDistict){
            if(condition.getDistinctCountList() == null || condition.getDistinctCountList().size() == 0){
                throw new IllegalArgumentException("distinctCountList不能为空！");
            }
            for(ExpandHbaseCell cell :condition.getDistinctCountList()){
                builder.addDistictColumns(HbaseUtil.toExpandCell(cell));
            }
        }
        
        return builder.build();
    }
    
    
    String AddValue(String v1,String v2,ByteString className){
        String c = className.toString();
        if("long".equals(c) || "Long".equals(c)){
            Long l1 = null;
            Long l2 = null;
            if(v1 != null )
                l1 = Long.parseLong(v1);
            if(v2 !=null)
                l2 = Long.parseLong(v2);
            if (l1 == null ^ l2 == null) {
                  return ((l1 == null) ? l2 : l1)+""; // either of one is null.
                } else if (l1 == null) // both are null
                  return null;
                return (Long)(l1 + l2)+"";
        }else if("int".equals(c) || "Integer".equals(c)){
            Integer i1 = null;
            Integer i2 = null;
            if(v1 != null )
                i1 = Integer.parseInt(v1);
            if(v2 !=null)
                i2 = Integer.parseInt(v2);
            if(i1 == null ^ i2 ==null){
                 return ((i1 == null) ? i2 : i1)+""; // either of one is null.
            }else if(i1 == null){
                return null;
            }
            return (i1+i2)+"";
        }else if("double".equals(c) || "Double".equals(c)){
            Double d1 = null;
            Double d2 = null;
            if(v1 != null) d1 = Double.parseDouble(v1);
            if(v2 != null) d2 = Double.parseDouble(v2);
            if (d1 == null ^ d2 == null) {
                return ((d1 == null) ? d2 : d1)+""; // either of one is null.
            }else if(d1 == null){
                return null;
            }
            return (d1+d2)+"";
        }else{
            BigDecimal b1 = null;
            BigDecimal b2 = null;
            if(v1 != null) b1 = BigDecimal.valueOf(Double.parseDouble(v1));
            if(v2 != null) b2 = BigDecimal.valueOf(Double.parseDouble(v2));
            if (b1 == null ^ b2 == null) {
                  return ((b1 == null) ? b2 : b1).toString(); // either of one is null.
                }
                if (b1 == null) {
                  return null;
                }
                return b1.add(b2).toString();
        }
    }
    /**
     * wxs 实现
     * @param table
     * @param condition
     * @param scan
     * @return
     * @throws Throwable
     */
     
    
	public List<ExpandHbaseRow> getGroupSum(HTable table,ExpandHbaseCondition condition,Scan scan) throws Throwable{
	    final ExpandAggregationRequest request = validateArgAndGetPB(condition, scan, true, true, true, false);
	    
	    class GroupSumAndCountCallback implements Batch.Callback<List<ExpandRow>>{
	        List<ExpandCell> sumList = request.getSumColumnsList();
	        List<ExpandCell> countList = request.getCountColumnsList();
	        List<ExpandHbaseRow> list = new ArrayList<ExpandHbaseRow>();
	        public List<ExpandHbaseRow> getGroupSumAndCount(){
	            return list;
	        }
	        /**
	        * @Title: update
	        * @Description: 
	        * @param 
	        * @return Batch.Callback<List<ExpandHbaseRow>>
	        * @author lpy
	        * @throws
	        */
	        public synchronized void update(byte[] region, byte[] row, List<ExpandRow> result) {
	            if(list.size() == 0){
	                if(result !=null){
	                    for(ExpandRow rowN : result){
	                        list.add(HbaseUtil.toExpandHbaseRow(rowN));
	                    }
	                }
	            }else{
	                List<ExpandHbaseRow> locat = new ArrayList<ExpandHbaseRow>();
	                if(result != null){
	                    for(ExpandRow rowN : result){
	                        locat.add(HbaseUtil.toExpandHbaseRow(rowN));
	                    }
	                }
	                for(ExpandHbaseRow listRow :  list){
	                    List<ExpandHbaseCell>  listCells = listRow.getKeys();
	                    for(ExpandHbaseRow locatRow : locat){
	                        List<ExpandHbaseCell> locatCells = locatRow.getKeys();
	                        if(listCells.size() == locatCells.size()){
	                            boolean flag = false;//假设不是同一个组
	                            for(int i=0;i<listCells.size();i++){
	                                //是同一个组
	                                if(listCells.get(i).getFamily().equals(locatCells.get(i).getFamily())&&
	                                        listCells.get(i).getQualify().equals(locatCells.get(i).getQualify()) &&
	                                        ((listCells.get(i).getValue() == null && locatCells.get(i).getValue() == null) ||
	                                                listCells.get(i).getValue()!=null && listCells.get(i).getValue().equals(locatCells.get(i).getValue()))){
	                                    flag = true;
	                                    break;
	                                }
	                            }
	                            if(flag){
	                                List<ExpandHbaseCell> listCol = listRow.getResultRow();
	                                List<ExpandHbaseCell> locatCol =locatRow.getResultRow();
	                                for(ExpandCell sumCell:sumList){
	                                    for(ExpandHbaseCell cell : listCol){
	                                        boolean check = false;
	                                        if(sumCell.getFamily().toStringUtf8().equals(cell.getFamily()) &&
	                                                sumCell.getQualify().toStringUtf8().equals(cell.getQualify())){
	                                            for(ExpandHbaseCell lcol : locatCol){
	                                                if(lcol.getFamily().equals(cell.getFamily()) && lcol.getQualify().equals(cell.getQualify())){
	                                                    String v = AddValue(cell.getValue(),lcol.getValue(),sumCell.getClassName());
	                                                    if(v==null) cell.setValue("0");
	                                                    else cell.setValue(v);
	                                                    check = true;
	                                                    break;
	                                                }
	                                            }
	                                        }
	                                        if(check) break;
	                                    }
	                                }
	                                
	                                for(ExpandCell countCell:countList){
	                                    for(ExpandHbaseCell cell : listCol){
	                                        boolean check = false;
	                                        if(countCell.getFamily().toStringUtf8().equals(cell.getFamily()) &&
	                                                countCell.getQualify().toStringUtf8().equals(cell.getQualify())){
	                                            for(ExpandHbaseCell lcol : locatCol){
	                                                if(lcol.getFamily().equals(cell.getFamily()) && lcol.getQualify().equals(cell.getQualify())){
	                                                    String v = AddValue(cell.getValue(),lcol.getValue(),ByteString.copyFromUtf8("Long"));
	                                                    if(v==null) cell.setValue("1");
	                                                    else cell.setValue(v);
	                                                    check = true;
	                                                    break;
	                                                }
	                                            }
	                                        }
	                                        if(check) break;
	                                    }
	                                }
	                            }else{
	                                list.add(locatRow);
	                            }
	                            
	                        }
	                    }
	                }
	                
	                locat.clear();
	            }
	            
	        }
	        
	    }
	    
	    
	    GroupSumAndCountCallback groupSumAndCountcallback = new GroupSumAndCountCallback();
	    table.coprocessorService(ExpandAggregationService.class, scan.getStartRow(), scan.getStopRow(), new Batch.Call<ExpandAggregationService, List<ExpandRow>>() {
	        /**
	        * @Title: call
	        * @Description: 
	        * @param return null;
	        * @return Batch.Call<ExpandAggregationService,List<ExpandRow>>
	        * @author lpy
	        * @throws
	        */
	        public List<ExpandRow> call(ExpandAggregationService instance) throws IOException {
	            ServerRpcController controller = new ServerRpcController();
	            BlockingRpcCallback<ExpandAggregationResponse> rpcCallback 
	                = new BlockingRpcCallback<ExpandAggregationResponse>();
	            instance.getGroupAndSum(controller, request, rpcCallback);//改为getGroupSum
	            ExpandAggregationResponse response = rpcCallback.get();
	             if (controller.failedOnException()) {
	                  throw controller.getFailedOn();
	             }
	             return response.getResultsList();
	        }
	
	        
	        
	    },groupSumAndCountcallback);
	    return groupSumAndCountcallback.getGroupSumAndCount();
	}
}
