import java.sql.Connection;
import java.sql.DriverManager;

public class MyConnection {
	private static String url = "jdbc:hive2://ubuntumaster:10000";
	private static String user = "ubuntu";//要是服务器的帐号
	private static String password = "123ubuntu";
	
	//获得链接
	public static Connection getHiveInstance() throws Exception{
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection con = DriverManager.getConnection(url, user,password);	
			return con;
	}
	
	
}
