package com.vrv.hdfs;


import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.vrv.hdfs.util.HDFSUtil;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsApplicationTests {

	private Logger logger  = Logger.getLogger(HdfsApplicationTests.class);
	
	@Autowired
	private HDFSUtil hdfsUtil;
	
	
	/**
	 * 初始化hadoop环境
	 */
	@Before
	public void initHadoopEnvironment(){
		System.setProperty("hadoop.home.dir", "D:\\tool\\hadoop\\winutil"); //windows版本
	}
	/**
	 * 获得文件
	 */
	@Test
	public void getFileSystem() {
		FileSystem fileSystem = hdfsUtil.getFileSystem();
		Configuration configuration = fileSystem.getConf();
		logger.info("configuration:"+configuration);		
	}
	
	/**
	 * 创建对应的目录
	 */
	@Test
	public void mkdir(){
		String path = "/wordcountInput";
		Boolean result = hdfsUtil.mkdir(path);
		Assert.assertEquals(result, true);
	}
	
	/**
	 * 删除目录
	 */
	@Test
	public void rmDir(){
		String path = "/wordcountInput";
		Boolean result = hdfsUtil.rmDir(path);
		Assert.assertEquals(result, true);
	}
	
	/**
	 * 将文件上传至HDFS上
	 */
	@Test
	public void copyFileToHDFS(){
		
	}
	
	
	

}

