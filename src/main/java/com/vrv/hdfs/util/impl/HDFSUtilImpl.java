package com.vrv.hdfs.util.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.vrv.hdfs.util.HDFSUtil;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2019年1月8日 下午10:22:54
* 类说明    HDFS工具类实现类
*/
@Component
public class HDFSUtilImpl implements HDFSUtil {
    
	private Logger logger  = Logger.getLogger(HDFSUtilImpl.class);
	
	@Value("${hdfsUrl}")
	private String hdfsUrl;
	
	
	@Override
	public FileSystem getFileSystem() {
		 Configuration conf = new Configuration();
		 conf.set("fs.defaultFS", hdfsUrl);
		 conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
		 conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		  FileSystem fs = null;
		 // 返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
		 if(StringUtils.isBlank(hdfsUrl)){
			 try {
				 fs = FileSystem.get(conf);
			} catch (IOException e) {
				logger.error("文件系统获取失败", e);
			}
		 }else{
			 // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
		        try {
		        	URI uri = new URI(hdfsUrl.trim());
		            fs = FileSystem.get(uri,conf);
		        } catch (URISyntaxException | IOException e) {
		            logger.error("文件系统获取失败", e);
		        }
		 }
		 return fs;
	}

	@Override
	public Boolean mkdir(String path) {
		Boolean result = false;
		FileSystem fileSystem = getFileSystem();
		String filePath = hdfsUrl+path;
		logger.info("filePath:"+filePath);
		try {
			fileSystem.mkdirs(new Path(filePath));
			result = true;
		} catch (IllegalArgumentException | IOException e) {
			logger.error("创建文件失败", e);
			result = false;
		}
		return result;
	}

	@Override
	public Boolean rmDir(String path) {
		Boolean result = false;
		FileSystem fileSystem = getFileSystem();
		String filePath = hdfsUrl+path;
		logger.info("filePath:"+filePath);
		try {
			fileSystem.delete(new Path(filePath), true);
			result = true;
		} catch (IOException e) {
			logger.error("创建文件失败", e);
			result = false;
		}
		return result;
	
	}

	@Override
	public Boolean copyFileToHDFS(Boolean delSrc, Boolean overwrite, String srcFile, String destPath){
		Boolean result = false;
		Path srcPath = new Path(srcFile);
	    if(StringUtils.isNotBlank(hdfsUrl)){
	        destPath = hdfsUrl + destPath;
	    }
	    Path dstPath = new Path(destPath);
	    // 实现文件上传
	    try {
	        //获取FileSystem对象
	        FileSystem fs = getFileSystem();
	        fs.copyFromLocalFile(srcPath, dstPath);
	        fs.copyFromLocalFile(delSrc,overwrite,srcPath, dstPath);
	        result = true;
	        //释放资源
	        fs.close();
	    } catch (IOException e) {
	        logger.error("文件上传失败", e);
	    }
		return result;
	}

	@Override
	public void getFile(String srcFile, String destFile) {
		// 源文件路径
	    if(StringUtils.isNotBlank(hdfsUrl)){
	        srcFile = hdfsUrl + srcFile;
	    }
	    Path srcPath = new Path(srcFile);
	    
	    // 目的路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/
	    Path dstPath = new Path(destFile);
	    try {
	        // 获取FileSystem对象
	        FileSystem fs = getFileSystem();
	        // 下载hdfs上的文件
	        fs.copyToLocalFile(srcPath, dstPath);
	        // 释放资源
	        fs.close();
	    } catch (IOException e) {
	        logger.error("下载文件失败", e);
	    }
	}

	@Override
	public DatanodeInfo[] getHDFSNodes() {
		// 获取所有节点
	    DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
	    try {
	        //返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        //获取分布式文件系统
	        DistributedFileSystem hdfs = (DistributedFileSystem)fs;
	        dataNodeStats = hdfs.getDataNodeStats();
	    } catch (IOException e) {
	        logger.error("获得HDFS集群节点信息", e);
	    }
	    return dataNodeStats;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(String filePath) {
		 // 文件路径
	    filePath = hdfsUrl + filePath;
	    Path path = new Path(filePath);
	    // 文件块位置列表
	    BlockLocation[] blkLocations = new BlockLocation[0];
	    try {
	        // 返回FileSystem对象
	        FileSystem fs = getFileSystem();
	        // 获取文件目录 
	        FileStatus filestatus = fs.getFileStatus(path);
	        //获取文件块位置列表
	        blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
	    } catch (IOException e) {
	        logger.error("获得文件位置失败", e);
	    }
	    return blkLocations;
	}

	@Override
	public Boolean reName(String srcPath, String dstPath) {
		 boolean flag = false;
		    try    {
		        // 返回FileSystem对象
		        FileSystem fs = getFileSystem();
		        srcPath = hdfsUrl + srcPath;   //重命令名称
		        dstPath = hdfsUrl + dstPath; 
		        flag = fs.rename(new Path(srcPath), new Path(dstPath));
		    } catch (IOException e) {
		        logger.error("hdfs重命令错误", e);
		    }
		    
		    return flag;
	}

	@Override
	public Boolean isExistDir(String filePath, Boolean create) {
		 boolean flag = false;
		    if (StringUtils.isEmpty(filePath)){
		        return flag;
		    }
		    try{
		        Path path = new Path(filePath);
		        // FileSystem对象
		        FileSystem fs = getFileSystem();
		        if (create){
		            if (!fs.exists(path)){
		                fs.mkdirs(path);
		            }
		        }
		        if (fs.isDirectory(path)){
		            flag = true;
		        }
		    }catch (Exception e){
		        logger.error("目录判断是够存在", e);
		    }
		    
		    return flag;
	}

}
