package sparkdemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;




public class Piestimation {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
							.setAppName("javasparkPi");
							
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		int slices = (args.length == 1)?Integer.parseInt(args[0]) : 2;
		int n = 10000*slices;
		List<Integer> list = new ArrayList<>(n);
		for(int i = 0; i < n; ++i)
			list.add(i);
		JavaRDD<Integer> dataSet = jsc.parallelize(list,slices);
		int count = dataSet.map(new Function<Integer, Integer>() {
			public Integer call(Integer integer) throws Exception {
				 double x = Math.random() * 2 - 1;
			     double y = Math.random() * 2 - 1;
			     return (x * x + y * y <= 1) ? 1 : 0;
			}}).reduce(new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer integer1, Integer integer2) throws Exception {
					return integer1 + integer2;
				}});
		System.out.println("Pi is roughly " + 4.0 * count / n);

	    jsc.stop();
	}

}
