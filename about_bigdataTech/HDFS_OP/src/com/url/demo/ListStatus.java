package com.url.demo;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.jcraft.jsch.IO;


//列出给定路径下的文件
public class ListStatus {
     static {
    	 URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
     }
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
			String url = args[0];
			Configuration conf = new Configuration();//Configuration对象封装了客户端或者服务器的配置，通过设置配置文件读取类路径来实现如core-site.xml
			FileSystem fs = FileSystem.get(URI.create(url),conf);//FileSystem是一个通用的文件系统，通过静态方法可以获取
			
			Path[] paths = new Path[args.length];//想要列出多少足路径集
			for(int i = 0; i<paths.length; i++)
			{
				paths[i] = new Path(args[i]);//指定一组路径
			}
			FileStatus[] status = fs.listStatus(paths);//将FileStatus存入路径数组中
			//fs.delete(Path f,boolean recursive);//可以永久性删除文件或目录，当f是文件或空目录的时候，recursive会被忽略，当recursive为true时非空目录将被删除
			Path[] listedPaths = FileUtil.stat2Paths(status);//stat2Paths()将一个FileStatus对象数组转换为一个Path对象数组
			for(Path p : listedPaths)//
			{
				System.out.println(p);
			}
			
	}

}
