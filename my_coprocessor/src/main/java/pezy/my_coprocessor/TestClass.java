package pezy.my_coprocessor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * 用于测试各种方法的临时类
 * @author wangxingshu
 *
 */
public class TestClass {

	public static void main(String[] args) {
		String str="sex";
		List<String> list=new ArrayList<String>();
		list.add("id");
		list.add("age");
		list.add("sex");
		//containTest(str, list);
//		HashSet<String> set=new HashSet<String>();
//		set.add("id");
//		set.add("age");
//		set.add("sex1");
//		set.add("sex2");
//		set.add("sex3");
//		for (String a : set) {
//			System.out.println(a);
//		}
//		System.out.println(set.contains("sex1"));
		ResultRow1 rr=new ResultRow1();
		System.out.println(rr.getValueCount());
		rr.addValueCount();
		System.out.println(rr.getValueCount());

	}
	public static void containTest(String str,List<String> collection){
		boolean result=collection.contains(str);
		System.out.println(result);
		
		
				
	}
}
class ResultRow1 {
	String keyColumn = null;
	int valueCount=0;
	public ResultRow1() {
		// TODO Auto-generated constructor stub
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