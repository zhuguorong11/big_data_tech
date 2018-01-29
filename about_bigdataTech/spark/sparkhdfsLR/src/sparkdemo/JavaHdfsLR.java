package sparkdemo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import java.util.regex.Pattern;

import org.antlr.grammar.v3.ANTLRv3Parser.range_return;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.ql.parse.HiveParser.alterStatementSuffixArchive_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.netlib.util.doubleW;

import scala.collection.generic.BitOperations.Int;



/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
 */
public final class JavaHdfsLR {
	
	private static final int D = 10;//维度
	private static final Random rand = new Random(42);//用来后面随机赋值权重
	
	//警告函数
	static void showWarning() {
	    String warning = "WARN: This is a naive implementation of Logistic Regression " +
	            "and is given as an example!\n" +
	            "Please use org.apache.spark.ml.classification.LogisticRegression " +
	            "for more conventional use.";
	    System.err.println(warning);
	  }
	
	//定义点，而且要进行序列化，保证读取正确
	static class DataPoint implements Serializable{
		public DataPoint(double[] x,double y) {
			this.x = x;
			this.y = y;
		}
		double[] x;
		double y;
	}
	
	//对读取到的每一行进行点的读取解析赋值ֵ
	static class ParsePoint implements Function<String, DataPoint>
	{
		private static final Pattern SPACE = Pattern.compile(" ");
		
		public DataPoint call(String line) throws Exception {
			String[] tok = SPACE.split(line);
			double y = Double.parseDouble(tok[0]);
			double[] x = new double[D];
			for(int i = 0; i<D;++i)
			{
					x[i] = Double.parseDouble(tok[i+1]);
			}
			return new DataPoint(x, y);
		}
	}
	
	//
	static class VectorSum implements Function2<double[],double[], double[]>
	{
		public double[] call(double[] a, double[] b) throws Exception {
			double[] res = new double[D];
			for (int i = 0;i<D;++i)
			{
				res[i] = a[i]+b[i];
			}
			return res;
		}
	}
	
	//计算梯度
	static class ComputeGradient implements Function<DataPoint, double[]>
	{
		private final double[] weights;
		public ComputeGradient(double[] weights) {
			this.weights = weights;
		} 
		public double[] call(DataPoint p) throws Exception {
			double[] gradient = new double[D];
		     for (int i = 0; i < D; i++) {
		        double dot = dot(weights, p.x);
		        gradient[i] = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y * p.x[i];
		      }
		      return gradient;
		}
	}

	//计算向量积
	public static double dot(double[] a, double[] b)
	{
		double x = 0;
		for(int i = 0 ;i < D;++i)
		{
			x += a[i]*b[i];
		}
		return x;
	}
	
	public static void printWeights(double[] a) {
		 System.out.println(Arrays.toString(a));
		 }
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 if (args.length < 2) {
		      System.err.println("Usage: JavaHdfsLR <file> <iters>");
		      System.exit(1);
		    }
		 
		 showWarning();
		 
		 SparkConf conf = new SparkConf().setAppName("JavaHdfsLR");
		 JavaSparkContext jsc = new JavaSparkContext(conf);
		 
		 JavaRDD<String> lines = jsc.textFile(args[0]);
		 
		 //通过ParsePoint对每一行进行解析
		 JavaRDD<DataPoint> points = lines.map(new ParsePoint()).cache();
		 
		 int ITERATIONS  = Integer.parseInt(args[1]);//迭代次数
		 
		// Initialize w to a random value
		 double[] w = new double[D];
		 for (int i = 0; i < D; i++) {
		    w[i] = 2 * rand.nextDouble() - 1;
		  }
		 
		 System.out.print("Initial w: ");
		 printWeights(w);
		 
		 for (int i = 1; i <= ITERATIONS; i++) {
		      System.out.println("On iteration " + i);

		    //每一次循环得到新的梯度
		      double[] gradient = points.map(new ComputeGradient(w))		
		    		  			.reduce(new VectorSum());
		    //对权重参数进行更新
		      for (int j = 0; j < D; j++) {
		          w[j] -= gradient[j];
		        }
		 }
		 System.out.print("Final w: ");
		 printWeights(w);
		 jsc.stop();
	}
}
