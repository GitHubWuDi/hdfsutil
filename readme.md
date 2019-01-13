# 完成hdfsutil使用说明引用
- （1）对应项目当中引入hdfs相关组件的manven仓库坐标坐标
           选用hadoop-client版本：
            
       ```xml
            <groupId>org.apache.hadoop</groupId>
		    <artifactId>hadoop-client</artifactId>
		    <version>2.7.3</version>
       ```
     maven引用仓库坐标：
            
       ```xml
       <dependency>
			<groupId>hdfs</groupId>
			<artifactId>hdfs</artifactId>
			<version>1.0</version>
		</dependency>
		
       ```
- (2) application.yml文件进行配置hdfs namenode url路径

     ```java
     hdfsUrl: hdfs://192.168.89.129:9000
     ````    
- (3) 使用service方法当中自动注入hdfs对应的类
       ```java
         @Autowired
	     private HDFSUtil hdfsUtil;
	   ``` 
- (4) 目前hdfs java api包括的对应的方法
 
      ```java
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
	 * @param create
	 * @return
	 */
	public Boolean isExistDir(String filePath, Boolean create);
        
      ```

      
            