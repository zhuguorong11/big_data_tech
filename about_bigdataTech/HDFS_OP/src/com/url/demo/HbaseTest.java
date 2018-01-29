package com.url.demo;

import java.util.List;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.client.Delete;  
public class HbaseTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	private HBaseAdmin admin = null;  
    // 定义配置对象HBaseConfiguration  
    private HBaseConfiguration cfg = null;  
  
    public HbaseTest() throws Exception {  
        Configuration HBASE_CONFIG = new Configuration();  
  
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.2.6");  
  
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");  
  
        cfg = new HBaseConfiguration(HBASE_CONFIG);  
  
        admin = new HBaseAdmin(cfg);  
    }  
  
    // 创建一张表，指定表名，列族  
    public void createTable(String tableName, String columnFarily)  
            throws Exception {  
  
        if (admin.tableExists(tableName)) {  
            System.out.println(tableName + "存在！");  
            System.exit(0);  
        } else {  
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);  
            tableDesc.addFamily(new HColumnDescriptor(columnFarily));  
            admin.createTable(tableDesc);  
            System.out.println("创建表成功！");  
        }  
    }  
  
    // Hbase获取所有的表信息  
    public List getAllTables() {  
        List<String> tables = null;  
        if (admin != null) {  
            try {  
                HTableDescriptor[] allTable = admin.listTables();  
                if (allTable.length > 0)  
                    tables = new ArrayList<String>();  
                for (HTableDescriptor hTableDescriptor : allTable) {  
                    tables.add(hTableDescriptor.getNameAsString());  
                    System.out.println(hTableDescriptor.getNameAsString());  
                }  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
        return tables;  
    }  
  
    // Hbase中往某个表中添加一条记录  
    public boolean addOneRecord(String table, String key, String family,  
            String col, byte[] dataIn) {  
        HTablePool tp = new HTablePool(cfg, 1000);  
        HTable tb = (HTable) tp.getTable(table);  
        Put put = new Put(key.getBytes());  
        put.add(family.getBytes(), col.getBytes(), dataIn);  
        try {  
            tb.put(put);  
            System.out.println("插入数据条" + key + "成功！！！");  
            return true;  
        } catch (IOException e) {  
            System.out.println("插入数据条" + key + "失败！！！");  
            return false;  
        }  
    }  
  
    // Hbase表中记录信息的查询  
    public void getValueFromKey(String table, String key) {  
        HTablePool tp = new HTablePool(cfg, 1000);  
        HTable tb = (HTable) tp.getTable(table);  
        Get get = new Get(key.getBytes());  
        try {  
            Result rs = tb.get(get);  
            if (rs.raw().length == 0) {  
                System.out.println("不存在关键字为" + key + "的行！!");  
  
            } else {  
                for (KeyValue kv : rs.raw()) {  
                    System.out.println(new String(kv.getKey()) + " "  
                            + new String(kv.getValue()));  
                }  
  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  
    // 显示所有数据，通过HTable Scan类获取已有表的信息  
    public void getAllData(String tableName) throws Exception {  
        HTable table = new HTable(cfg, tableName);  
        Scan scan = new Scan();  
        ResultScanner rs = table.getScanner(scan);  
        for (Result r : rs) {  
            for (KeyValue kv : r.raw()) {  
                System.out.println(new String(kv.getKey())  
                        + new String(kv.getValue()));  
            }  
        }  
    }  
  
    // Hbase表中记录信息的删除  
    public boolean deleteRecord(String table, String key) {  
        HTablePool tp = new HTablePool(cfg, 1000);  
        HTable tb = (HTable) tp.getTable(table);  
        Delete de = new Delete(key.getBytes());  
        try {  
            tb.delete(de);  
            return true;  
        } catch (IOException e) {  
            System.out.println("删除记录" + key + "异常！！！");  
            return false;  
        }  
    }  
  
    // Hbase中表的删除  
    public boolean deleteTable(String table) {  
        try {  
            if (admin.tableExists(table)) {  
                admin.disableTable(table);  
                admin.deleteTable(table);  
                System.out.println("删除表" + table + "!!!");  
            }  
            return true;  
        } catch (IOException e) {  
            System.out.println("删除表" + table + "异常!!!");  
            return false;  
        }  
    }  
}
