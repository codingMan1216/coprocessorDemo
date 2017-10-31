package groupByDemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public class CountDemo {
	// static ApplicationContext context = new
	// ClassPathXmlApplicationContext(new String[] {
	// "spring-hbase.xml","applicationContext.xml"});
	// static BeanFactory factory = (BeanFactory) context;
	// static HbaseTemplateBaseDao hbaseTemplate = (HbaseTemplateBaseDao)
	// factory.getBean("hbaseTemplate");
	//
	public static void main(String[] args) {
		Connection connection = null;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.31.101,172.16.31.102,172.16.31.103");
		try {
			connection = ConnectionFactory.createConnection(conf);
			TableName tableName = TableName.valueOf("wxs0920");
			Scan scan = new Scan();
			ExpandAggregationClient ac = new ExpandAggregationClient(conf);
			scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"));
			scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("num"));
			scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
			// AggregationClient ac1 = new
			// AggregationClient(hbaseTemplate.getConfiguration());
			Long lo = 0L;
			ExpandHbaseCondition hbase = new ExpandHbaseCondition();
			List<ExpandHbaseCell> sumList = new ArrayList<ExpandHbaseCell>();
			ExpandHbaseCell cell = new ExpandHbaseCell("id", "num", "sex");
			sumList.add(cell);
			hbase.setSumList(sumList);
			cell = new ExpandHbaseCell("cf", "id");
			// ExpandHbaseCell cell2 = new ExpandHbaseCell("info", "age");
			List<ExpandHbaseCell> group = new ArrayList<ExpandHbaseCell>();
			group.add(cell);
			// group.add(cell2);
			hbase.setGroupList(group);
			cell = new ExpandHbaseCell("cf", "sex");
			List<ExpandHbaseCell> count = new ArrayList<ExpandHbaseCell>();
			count.add(cell);
			hbase.setCountList(count);
			// hbase.setDistinctCountList(group);
			List<ExpandHbaseCell> distinct = new ArrayList<ExpandHbaseCell>();
			ExpandHbaseCell cell1 = new ExpandHbaseCell("cf", "id");
			ExpandHbaseCell cell2 = new ExpandHbaseCell("cf", "sex");
			distinct.add(cell1);
			distinct.add(cell2);
			hbase.setDistinctCountList(distinct);
			List<ExpandHbaseRow> result = null;
			try {
				// lo = ac1.rowCount(tableName, new LongColumnInterpreter(), s);
				result = ac.getGroupSumAndCount("wxs0920", hbase, scan);
			
				System.out.println(result == null ? "bug" : result.size());
				// System.out.println(lo);
				for (ExpandHbaseRow row : result) {
					String context = "";
					List<ExpandHbaseCell> ll = row.getResultRow();
					if (ll != null)
						for (ExpandHbaseCell c : ll) {
							context += c.getFamily() + ":" + c.getQualify() + "--->";
							context += c.getValue();
							context += "\t";
						}
					System.out.println(context);

				}

//				result = ac.getDistinctCount("wxs0920", hbase, scan);
//				System.out.println(result == null ? "bug" : result.size());
//				for (ExpandHbaseRow row : result) {
//					String context = "";
//					List<ExpandHbaseCell> ll = row.getResultRow();
//					if (ll != null)
//						for (ExpandHbaseCell c : ll) {
//							context += c.getFamily() + ":" + c.getQualify() + "--->";
//							context += c.getValue();
//							context += "\t";
//						}
//					System.out.println(context);
//				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
			System.out.println(lo);
		} catch (Exception e) {
			e.printStackTrace();

		}finally {
			try {
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
