package groupByDemo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.protobuf.ByteString;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandCell;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandRow;
public class HbaseUtil {
	 public static void getEntityProperties(Object object , Method method,Object param,byte[] value) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException{
	        if (param.equals(Integer.class)) {
	                Integer val = Integer.valueOf(Bytes.toString(value));
	                method.invoke(object, val);
	           } else if (param.equals(String.class)) {
	               String val = Bytes.toString(value);
	               method.invoke(object, val);
	           } else if (param.equals( Double.class)) {
	               Double val = Double.valueOf(Bytes.toString(value));
	               method.invoke(object, val);
	           } else if (param.equals(Float.class)) {
	               Float val = Float.valueOf(Bytes.toString(value));
	               method.invoke(object, val);
	           } else if (param.equals(Long.class)) {
	               Long val = Long.getLong(Bytes.toString(value));
	               method.invoke(object, val);
	           } else if (param.equals(Boolean.class)) {
	               Boolean val = Boolean.valueOf(Bytes.toString(value));
	               method.invoke(object, val);
	           } else if (param instanceof Date) {
	               
	           } else if(param instanceof Map){
	               
	           } 
	    }
	    /**
	     * 把hbase的字段转成Java字段，java中用驼峰命名法
	     * @param name
	     * @return
	     */
	    public static String getEntityPropertiesName(String name){
	        String newName = "";
	        boolean flag = false;
	        for(int i=0;i<name.length();i++){
	            char a = name.charAt(i);
	            if(a=='_' || a== '-'){
	                flag = true;
	                continue;
	            }
	            if(flag){
	                if(a >= 'a' && a<= 'z' ){
	                    a = (char) (a - 32);
	                }
	                flag = false;
	            }
	            newName += a;
	        }
	        return newName;
	    }
	    
	    public static String getfirstUp(String name){
	        String newName = "";
	        String a = "";
	        if(name != null && name.length() > 0){
	            a = name.substring(0, 1);
	            a = a.toUpperCase();
	        }
	        if(name != null && name.length() > 1)
	            newName = a + name.substring(1,name.length());
	        else
	            return a;
	        return newName;
	    }
	    
	    /**
	     * 将ExpandHbaseCell对象转换成ExpandCell对象
	     * @param cell
	     * @return
	     */
	    public static ExpandCell toExpandCell(ExpandHbaseCell cell){
	        if(cell == null) return null;
	      //  AssertPropertiseException.notNull(cell.getFamily(), "列簇不能为空！");
	       // AssertPropertiseException.notNull(cell.getQualify(), "列不能为空！");
	        ExpandCell.Builder builder = ExpandCell.newBuilder();
	        builder.setFamily(ByteString.copyFromUtf8(cell.getFamily()));
	        builder.setQualify(ByteString.copyFromUtf8(cell.getQualify()));
	        if(cell.getValue() != null)
	            builder.setValue(ByteString.copyFromUtf8(cell.getValue()));
	        if(cell.getClassName() != null)
	            builder.setClassName(ByteString.copyFromUtf8(cell.getClassName()));
	        return builder.build();
	    }
	    
	    /**
	     * 将ExpandCell对象转换成ExpandHbaseCell对象 
	     * @param cell
	     * @return
	     */
	    public static ExpandHbaseCell toExpandCell(ExpandCell cell){
	        if(cell == null) return null;
//	        AssertPropertiseException.notNull(cell.getFamily(), "列簇不能为空！");
//	        AssertPropertiseException.notNull(cell.getQualify(), "列不能为空！");
	        ExpandHbaseCell nCell = new ExpandHbaseCell();
	        nCell.setFamily(cell.getFamily().toStringUtf8());
	        nCell.setQualify(cell.getQualify().toStringUtf8());
	        if(cell.getValue() != null)
	            nCell.setValue(cell.getValue().toStringUtf8());
	        if(cell.getClassName() != null)
	            nCell.setClassName(cell.getClassName().toStringUtf8());
	        if(cell.getDistinctValuesList()!=null && cell.getDistinctValuesList().size() > 0){
	            List<ByteString> byteList = cell.getDistinctValuesList();
	            for(ByteString s:byteList){
	                if(s == null){
	                    nCell.getValues().add(null);
	                }else{
	                    nCell.getValues().add(s.toStringUtf8());
	                }
	            }
	        }
	        return nCell;
	    }
	    /**
	     * 将ExpandRow对象转换成ExpandHbaseRow对象
	     * @param row
	     * @return
	     */
	    public static ExpandHbaseRow toExpandHbaseRow(ExpandRow row){
	        if(row==null) return null;
	        if(row.getValuesList().size()==0) return null;
	        ExpandHbaseRow hbaseRow = new ExpandHbaseRow();
	        List<ExpandHbaseCell> hbaseCells = new ArrayList<ExpandHbaseCell>();
	        for(ExpandCell cell:row.getValuesList()){
	            ExpandHbaseCell hbaseCell = HbaseUtil.toExpandCell(cell);
	            hbaseCells.add(hbaseCell);
	        }
	        hbaseRow.setResultRow(hbaseCells);
	        return hbaseRow;
	        
	    }
}
