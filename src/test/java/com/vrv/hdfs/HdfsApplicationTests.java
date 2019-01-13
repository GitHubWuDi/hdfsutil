package com.vrv.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
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
	 * 获得文件（通过）
	 */
	@Test
	public void getFileSystem() {
		FileSystem fileSystem = hdfsUtil.getFileSystem();
		Configuration configuration = fileSystem.getConf();
		logger.info("configuration:"+configuration);		
	}
	
	/**
	 * 创建对应的目录（通过）
	 */
	@Test
	public void mkdir(){
		String path = "/wordcountInput";
		Boolean result = hdfsUtil.mkdir(path);
		Assert.assertEquals(result, true);
	}
	
	/**
	 * 删除目录（通过）
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
		String srcFile = "D:\\tmp\\file\\kafkatest.txt";
		String destPath = "/wordcountInput";
		Boolean result = hdfsUtil.copyFileToHDFS(false, false, srcFile, destPath);
		Assert.assertEquals(result, true);
	}
	
	/**
	 * 将hdfs上的文件下载到对应的系统目录下
	 */
	@Test
	public void getFile() {
		String srcFile = "/wordcountInput";
		String destFile = "/usr/local/hadoop/hadoop-2.9.2/etc/hadoop";
		hdfsUtil.getFile(srcFile, destFile);
		Assert.assertEquals(true, true);
	}
	
	/**
	 * 获得hdfs集群节点
	 */
	@Test
	public void getHDFSNodes(){
		DatanodeInfo[] datanodeInfos = hdfsUtil.getHDFSNodes();
		Assert.assertEquals(3, datanodeInfos.length);
	}
	
	/**
	 *查找某个文件在 HDFS集群的位置 
	 */
	@Test
	public void getFileBlockLocations(){
		String filePath = "/wordcountInput";
		BlockLocation[] blockLocations = hdfsUtil.getFileBlockLocations(filePath);
		Assert.assertEquals(0, blockLocations.length);
	}
	/**
	 * 文件重命名（通过）
	 * @param srcPath
	 * @param dstPath
	 */
	@Test
	public void reName(){
		String srcPath = "/wordcountInput";
		String dstPath = "/wordcountInputs";
		Boolean reName = hdfsUtil.reName(srcPath, dstPath);
		Assert.assertEquals(reName, true);
	}
	
	/**
	 * 判断文件路径是否存在（通过）
	 */
	@Test
	public void isExistDir(){
		String srcPath = "/wordcountInput";
		Boolean existDir = hdfsUtil.isExistDir(srcPath, true);
		Assert.assertEquals(existDir, true);
	}

}

