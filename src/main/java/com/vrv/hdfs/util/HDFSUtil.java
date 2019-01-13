package com.vrv.hdfs.util;
/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2019年1月8日 下午4:33:10
* 类说明 HDFS工具类
*/

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public interface HDFSUtil {
	/**
	 * 获取文件系统
	 * @return
	 */
	public FileSystem getFileSystem();
	
	/**
	 * 创建文件目录
	 * @param path
	 */
	public Boolean mkdir(String path);
	
	/**
	 * 删除文件或者是某个目录
	 * @param path
	 * @return
	 */
	public Boolean rmDir(String path);
	
	/**
	 * 文件上传
	 * @param delSrc 是否删除源文件
	 * @param overwrite 是否覆盖原文件
	 * @param srcFile   源文件
	 * @param destPath  目的路径
	 * @return
	 */
	public Boolean copyFileToHDFS(Boolean delSrc, Boolean overwrite,String srcFile,String destPath);
	
	
	/**
	 * 从HDFS下载文件
	 * @param srcFile
	 * @param destFile
	 */
	public void getFile(String srcFile,String destFile);
	
	/**
	 * 获取HDFS集群节点信息
	 * @return
	 */
	public DatanodeInfo[] getHDFSNodes();
	
	/**
	 * 查找某个文件在 HDFS集群的位置
	 * @param filePath
	 * @return
	 */
	public BlockLocation[] getFileBlockLocations(String filePath);
	
	/**
	 * HDFS文件重命令
	 * @param srcPath
	 * @param dstPath
	 * @return
	 */
	public Boolean reName(String srcPath, String dstPath);
	
	
	/**
	 * 判断目录是否存在
	 * @param filePath
	 * @param create: 是否自动创建该目录
	 * @return
	 */
	public Boolean isExistDir(String filePath, Boolean create);
}
