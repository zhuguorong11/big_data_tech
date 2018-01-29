package zgrHbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.xml.sax.HandlerBase;

import com.google.inject.Key;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class HbaseTest {
	   //private static final Log LOG = LogFactory.getLog(HbaseTest.class);
	    // 在Eclipse中运行时报错如下
	    //     Caused by: java.lang.ClassNotFoundException: org.apache.htrace.Trace
	    //     Caused by: java.lang.NoClassDefFoundError: io/netty/channel/ChannelHandler
	    // 需要把单独的htrace-core-3.1.0-incubating.jar和netty-all-4.0.5.final.jar导入项目中
	       

	    //private static final String COLUMN_FAMILY_NAME = "cf";
	    static Configuration conf = null;
	    public  HbaseTest() {
	        conf = HBaseConfiguration.create();
//	        conf.set("hbase.master", "nnode:60000");
//	        conf.set("hbase.zookeeper.property.clientport", "2181");
//	        conf.set("hbase.zookeeper.quorum", "nnode,dnode1,dnode2");
	    }
	    /**
	     * @param args
	     */
	    public static void main(String[] args) {
	        HbaseTest manageMain = new HbaseTest();
	        try {
	            /**
	             * HTable类读写时是非线程安全的，已经标记为Deprecated
	             * 建议通过org.apache.hadoop.hbase.client.Connection来获取实例
	             */
	            Connection connection = ConnectionFactory.createConnection(conf);
	            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
//	            /**
//	             *  列出所有的表
//	             */
//	            manageMain.listTables(admin);
//	             
	            /**
	             * 判断表m_domain是否存在
	             */
//	            boolean exists = manageMain.isExists(admin,"t1");
//	             
//	            /**
//	             * 存在就删除
//	             */
//	            if (exists) {
//	                manageMain.deleteTable(admin,"t1");
//	            } 
	             
//	            /**
//	             * 创建表
//	             */
//	            manageMain.createTable(admin,"t1","f1");
//	             
//	            /**
//	             *  再次列出所有的表
//	             */
//	            manageMain.listTables(admin);
	             
	            /**
	             * 添加数据
	             */
//	            manageMain.putDatas(connection,"t1","f1");
//	             
//	            /**
//	             * 检索数据-表扫描
//	             */
	        //   manageMain.scanTable(connection,"t1","f1");
//	             
//	            /**
//	             * 检索数据-单行读
//	             */
	            manageMain.getData(connection,"t1","f1");
//	             
	            /**
	             * 获取一列数据	       
                 */
	            manageMain.getCol(connection, "t1", "f1");
//	            /**
//	             * 检索数据-根据条件
//	             */
//	            manageMain.queryByFilter(connection);
//	             
//	            /**
//	             * 删除数据
//	             */
//	            manageMain.deleteDatas(connection,"t1","f1");
//	            manageMain.scanTable(connection,"t1","f1");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	         
	    }
	 
	    /**
	     * 列出表
	     * @param admin
	     * @throws IOException 
	     */
	    private void listTables (Admin admin) throws IOException {
	        TableName [] names = admin.listTableNames();
	        for (TableName tableName : names) {
	            System.out.println("Table Name is : " + tableName.getNameAsString());
	        }
	        System.out.println("查询结束");
	    }
	     
	    /**
	     * 判断表是否存在
	     * @param admin StableName
	     * @return
	     * @throws IOException
	     */
	    private boolean isExists (Admin admin,String StableName) throws IOException {
	        /**
	         * org.apache.hadoop.hbase.TableName为为代表了表名字的Immutable POJO class对象,
	         * 形式为<table namespace>:<table qualifier>。
	         *   static TableName  valueOf(byte[] fullName) 
	         *  static TableName valueOf(byte[] namespace, byte[] qualifier) 
	         *  static TableName valueOf(ByteBuffer namespace, ByteBuffer qualifier) 
	         *  static TableName valueOf(String name) 
	         *  static TableName valueOf(String namespaceAsString, String qualifierAsString) 
	         * HBase系统默认定义了两个缺省的namespace
	         *     hbase：系统内建表，包括namespace和meta表
	         *     default：用户建表时未指定namespace的表都创建在此
	         * 在HBase中，namespace命名空间指对一组表的逻辑分组，类似RDBMS中的database，方便对表在业务上划分。
	         * 
	        */ 
	        TableName tableName = TableName.valueOf(StableName);
	         
	        boolean exists = admin.tableExists(tableName);
	        if (exists) {
	            System.out.print("Table " + tableName.getNameAsString() + " already exists.");
	        } else {
	        	System.out.print("Table " + tableName.getNameAsString() + " not exists.");
	        }
	        return exists;
	    }
	     
	    /**
	     * 创建表
	     * @param admin StableName colName
	     * @throws IOException
	     */
	    private void createTable (Admin admin,String StableName,String colName) throws IOException {
	        TableName tableName = TableName.valueOf(StableName);
	        System.out.print("To create table named " + StableName);
	        HTableDescriptor tableDesc = new HTableDescriptor(tableName);//建表
	        HColumnDescriptor columnDesc = new HColumnDescriptor(colName);//列簇名
	        tableDesc.addFamily(columnDesc);//将簇名添加到表中
	         
	        admin.createTable(tableDesc);//创建表
	        System.out.println("表创建成功");
	    }
	     
	    /**
	     * 删除表
	     * @param admin StableName
	     * @throws IOException
	     */
	    private void deleteTable (Admin admin,String StableName) throws IOException {
	        TableName tableName = TableName.valueOf(StableName);
	        System.out.println("disable and then delete table named " + StableName);
	        admin.disableTable(tableName);//删除表之前先把是表处于disable状态
	        admin.deleteTable(tableName);
	        System.out.println("表删除成功");
	    }
	     
	    /**
	     * 添加数据
	     * @param connection StableName colName
	     * @throws IOException
	     */
	    private void putDatas (Connection connection,String StableName,String colName) throws IOException {
	        String [] rows = {"baidu.com_19991011_20151011", "alibaba.com_19990415_20220523"};//需要添加的行号
	        String [] columns = {"owner", "ipstr", "access_server", "reg_date", "exp_date"};
	        String [][] values = {
	            {"Beijing Baidu Technology Co.", "220.181.57.217", "北京", "1999年10月11日", "2015年10月11日"}, 
	            {"Hangzhou Alibaba Advertising Co.", "205.204.101.42", "杭州", "1999年04月15日", "2022年05月23日"}
	        };
	        TableName tableName = TableName.valueOf(StableName);
	        byte [] family = Bytes.toBytes(colName);
	        Table table = connection.getTable(tableName);
	        for (int i = 0; i < rows.length; i++) {
	            System.out.println("========================" + rows[i]);
	            byte [] rowkey = Bytes.toBytes(rows[i]);
	            Put put = new Put(rowkey);//添加行键
	            for (int j = 0; j < columns.length; j++) {
	                byte [] qualifier = Bytes.toBytes(columns[j]);//添加列
	                byte [] value = Bytes.toBytes(values[i][j]);//添加列值
	                put.addColumn(family, qualifier, value);//把该列加入到列簇中 还可以使用put.add
	            }
	            table.put(put);//表中加入该行
	        }
	        System.out.println("数据添加成功");
	        table.close();
	    }
	     
	    /**
	     * 检索数据-单行获取
	     * @param connection StableName colName
	     * @throws IOException 
	     */
	    private void getData(Connection connection,String StableName,String colName) throws IOException {
	    	System.out.println("Get data from table " + StableName + " by family.");
	        TableName tableName = TableName.valueOf(StableName);
	        byte [] family = Bytes.toBytes(colName);//簇列
	        Table table = connection.getTable(tableName);
	         
	        byte [] row = Bytes.toBytes("baidu.com_19991011_20151011");//根据行键来获取
	        Get get = new Get(row);
	        get.addFamily(family);
	        // 也可以通过addFamily或addColumn来限定查询的数据
	        Result result = table.get(get);//得到返回的结果集
	        List<Cell> cells = result.listCells();
	        //List<KeyValue> list = result.list();
	        for (Cell cell : cells) {
	            String qualifier = new String(CellUtil.cloneQualifier(cell));
	            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
	            // @Deprecated
	            // LOG.info(cell.getQualifier() + "\t" + cell.getValue());
	            System.out.println(qualifier + "\t" + value);
	        }
//	        Iterator<KeyValue> iterator = list.iterator();
//	        while(iterator.hasNext()){
//	        	KeyValue kValue = iterator.next();
//	        	String key = new String(kValue.getKey(), "UTF-8");取出的键
//	        	String val = new String(kValue.getValue(), "UTF-8");
//	        	System.out.println(key+":"+val);
//	        }
	         
	    }
	    /**
	     * 获取一列
	     * @param connection StableName colName
	     * @throws IOException 
	     */
	    private void getCol(Connection connection,String StableName,String colName) throws IOException {
	    	System.out.println("Scan table " + StableName + " to browse all datas.");
	        TableName tableName = TableName.valueOf(StableName);
	        byte [] family = Bytes.toBytes(colName);
	         
	        Scan scan = new Scan();
	        scan.addFamily(family);//通过列簇
	         
	        Table table = connection.getTable(tableName);//通过连接得到表
	        ResultScanner resultScanner = table.getScanner(scan);//扫描表，得到扫描结果集
	        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext(); ) {//进行遍历
	            Result result = it.next();
	            //result.containsColumn(family, qualifier);检查指定的列是否存在
	            String date = new String(result.getValue(family, Bytes.toBytes("reg_date")),"UTF-8");
	            String ip = new String(result.getValue(family, Bytes.toBytes("ipstr")));
	            System.out.println(ip);
	            System.out.println(date);
	        }
	        System.out.println("查询结束");
	    }
	    
	    
	    /**
	     * 检索数据-表扫描
	     * @param connection StableName colName
	     * @throws IOException 
	     */
	    private void scanTable(Connection connection,String StableName,String colName) throws IOException {
	    	System.out.println("Scan table " + StableName + " to browse all datas.");
	        TableName tableName = TableName.valueOf(StableName);
	        byte [] family = Bytes.toBytes(colName);
	         
	        Scan scan = new Scan();
	        scan.addFamily(family);//通过列簇
	         
	        Table table = connection.getTable(tableName);//通过连接得到表
	        ResultScanner resultScanner = table.getScanner(scan);//扫描表，得到扫描结果集
	        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext(); ) {//进行遍历
	            Result result = it.next();
	            List<Cell> cells = result.listCells();//获得一个行键
	            for (Cell cell : cells) {//对一个单元cell进行遍历
	                String qualifier = new String(CellUtil.cloneQualifier(cell));
	                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
	                // @Deprecated
	                // LOG.info(cell.getQualifier() + "\t" + cell.getValue());
	                System.out.println(qualifier + "\t" + value);
	            }
	        }
	    }
	 
	    /**
	     * 安装条件检索数据
	     * @param connection
	     */
	    private void queryByFilter(Connection connection) {
	        // 简单分页过滤器示例程序
	        Filter filter = new PageFilter(15);     // 每页15条数据
	        int totalRows = 0;
	        byte [] lastRow = null;
	         
	        Scan scan = new Scan();
	        scan.setFilter(filter);
	         
	        // 略遍历，扫描等
	    }
	     
	    /**
	     * 删除数据
	     * @param connection StableName colName
	     * @throws IOException 
	     */
	    private void deleteDatas(Connection connection,String StableName,String colName) throws IOException {
	    	System.out.println("delete data from table " + StableName + " .");
	        TableName tableName = TableName.valueOf(StableName);
	        byte [] family = Bytes.toBytes(colName);
	        byte [] row = Bytes.toBytes("baidu.com_19991011_20151011");
	        Delete delete = new Delete(row);
	         
	        // @deprecated Since hbase-1.0.0. Use {@link #addColumn(byte[], byte[])}
	        // delete.deleteColumn(family, qualifier);            // 删除某个列的某个版本
	        delete.addColumn(family, Bytes.toBytes("owner"));//把该行先的owner删除delete.addColumns可以删除多个
	 
	        // @deprecated Since hbase-1.0.0. Use {@link #addColumns(byte[], byte[])}
	        // delete.deleteColumns(family, qualifier)            // 删除某个列的所有版本
	         
	        // @deprecated Since 1.0.0. Use {@link #(byte[])}
	        // delete.addFamily(family);                           // 删除某个列族
	        
	        Table table = connection.getTable(tableName);
	        table.delete(delete);
	        System.out.println("删除数据成功");
	        connection.close();
	    }
}
