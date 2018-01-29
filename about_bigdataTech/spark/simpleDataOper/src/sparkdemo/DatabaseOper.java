package sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.jamon.escaping.JavascriptEscaping;

public class DatabaseOper {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String url =
				  "jdbc:mysql://ubuntumaster:3306/zgr?user=root;password=118925";
		SparkConf conf = new SparkConf().setAppName("jdbcOp");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read().format("jdbc").option("url", url)
								 .option("dbtable", "people").load();
		
		df.printSchema();
		
		DataFrame countsByAge = df.groupBy("age").count();
		
		countsByAge.show();
		
		countsByAge.write().format("json").save(args[0]);
	}

}
