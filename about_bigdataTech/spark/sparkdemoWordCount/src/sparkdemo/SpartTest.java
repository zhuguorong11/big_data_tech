package sparkdemo;



import java.util.Arrays;
import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;



public class SpartTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("wordcount");
		JavaSparkContext sc = new JavaSparkContext(conf);
														
		//wordcount
		//引入文件
		JavaRDD<String> textFile = sc.textFile(args[0]);
		
		//进行分词
		/** 
         * new FlatMapFunction<String, String>两个string分别代表输入和输出类型 
         * Override的call方法需要自己实现一个转换的方法，并返回一个Iterable的结构 
         * 
         * flatmap属于一类非常常用的spark函数，简单的说作用就是将一条rdd数据使用你定义的函数给分解成多条rdd数据 
         * 例如，当前状态下，lines这个rdd类型的变量中，每一条数据都是一行String，我们现在想把他拆分成1个个的词的话， 
         * */
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" "));
			}});
		
		//进行分词统计
		/*
		 * 类似于MR的map方法 
         * pairFunction<T,K,V>: T:输入类型；K,V：输出键值对 
         * 需要重写call方法实现转换 
		 * */
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String,String, Integer>(){
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}});
		
		//进行合并
		/*
		 * reduceByKey方法，类似于MR的reduce 
         *  要求被操作的数据（即下面实例中的pairs）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算 
         *  
         *  Function2(T1,T2,R) arguments of type T1 and T2 and returns an R.
		 * */
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}});
		
		//进行排序
		JavaPairRDD<String, Integer> sort = counts.sortByKey();
		
		//写入
		//collect()该方法用于将spark的RDD类型转化为我们熟知的java常见类型
		counts.saveAsTextFile(args[1]);
		List<Tuple2<String, Integer>> output = sort.collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + ": " + tuple._2());
	    }
	    sc.stop();
	  
	}

}
