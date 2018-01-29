import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class HiveDemo {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://ubuntumaster:10000";
	private static String user = "APP";
	private static String password = "mine";
	private static String sql = "";
	private static ResultSet res;
	private static final Logger log = Logger.getLogger(HiveDemo.class);

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConn();
			stmt = conn.createStatement();

//			// 第一步:存在就先删除
//			String tableName = dropTable(stmt,"page_view");

			// 第二步:不存在就创建
		//	createTable(stmt, "zgr2");

			// 第三步:查看创建的表
    		showTables(stmt);
//
//			// 执行describe table操作
//			describeTables(stmt, tableName);

//			// 执行load data into table操作
//			loadData(stmt, tableName);
//
//			// 执行 select * query 操作
//			selectData(stmt, tableName);
//
//			// 执行 regular hive query 统计操作
//			countData(stmt, tableName);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.error(driverName + " not found!", e);
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("Connection error!", e);
			System.exit(1);
		} finally {
			try {
				if (conn != null) {
					conn.close();
					conn = null;
				}
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static void countData(Statement stmt, String tableName) throws SQLException {
		sql = "select count(1) from " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行“regular hive query”运行结果:");
		while (res.next()) {
			System.out.println("count ------>" + res.getString(1));
		}
	}

	private static void selectData(Statement stmt, String tableName) throws SQLException {
		sql = "select * from " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行 select * query 运行结果:");
		while (res.next()) {
			System.out.println(res.getInt(1) + "\t" + res.getString(2));
		}
	}

	private static void loadData(Statement stmt, String tableName) throws SQLException {
		String filepath = "/home/hadoop01/data";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("数据导入成功");
	}

	private static void describeTables(Statement stmt, String tableName) throws SQLException {
		sql = "describe " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行 describe table 运行结果:");
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
	}

	private static void showTables(Statement stmt) throws SQLException {
		sql = "show tables";
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行 show tables 运行结果:");
		if (res.next()) {
			System.out.println(res.getString(1));
		}
	}

	private static void createTable(Statement stmt, String tableName) throws SQLException {
		sql = "create table " + tableName + " (key int, value string)  row format delimited fields terminated by '\t'";
		stmt.execute(sql);
		System.out.println("表创建成功");
	}

	private static String dropTable(Statement stmt,String tableName) throws SQLException {
		sql = "drop table " + tableName;
		stmt.execute(sql);
		System.out.println("表删除成功");
		return tableName;
	}

	private static Connection getConn() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}

}
