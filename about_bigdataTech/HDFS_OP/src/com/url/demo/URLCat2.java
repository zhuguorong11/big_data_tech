package com.url.demo;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.jcraft.jsch.IO;

//读取hdfs上的文件
public class URLCat2 {
     static {
    	 URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
     }
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
			InputStream  in = null;
			String url = args[0];
			Configuration conf = new Configuration();//Configuration对象封装了客户端或者服务器的配置，通过设置配置文件读取类路径来实现如core-site.xml
			FileSystem fs = FileSystem.get(URI.create(url),conf);//FileSystem是一个通用的文件系统，通过静态方法可以获取
			//FileSystem具有很多操作
			try {
		         in = fs.open(new Path(url));//通过open函数来获取文件的输入流
		         IOUtils.copyBytes(in, System.out, 4096,false);
		         
		         
			} finally {
				// TODO: handle finally clause
				IOUtils.closeStream(in);
			}
	}

}
