package com.aliyun.odps.examples.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

/* 
 * 该示例展示了如何在MapReduce程序中读取文件资源
 * 
 * 该示例主要用于演示Local模式下的调试，如果要将该示例运行于在线环境，请将
 * main方法中的语句 job.setResources("file_resource.txt"); 删除
 * 
 *  Usage:
 *       Set Resource arguments: file_resource.txt 
 *       Set program arguments: wc_in1 rs_out
 */
public class Resource {

  public static class TokenizerMapper extends MapperBase {
    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      long count = 0;

      InputStream in = context.readResourceFileAsStream("file_resource.txt");
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = br.readLine()) != null) {
        count++;
      }
      br.close();

      result.set(0, "file_resource");
      result.set(1, count);
      context.write(result);

    }
  }

  public static void main(String[] args) throws Exception {
    
    if (args.length != 2) {
      System.err.println("Usage:");
      System.err.println("\t Set Resource arguments: file_resource.txt");
      System.err.println("\t Set program arguments: wc_in1 rs_out");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    InputUtils.addTable(TableInfo.builder().tableName("wc_in1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("rs_out").build(), job);

    ////////////////在线环境删除该行代码///////////////////
    
    job.setResources("file_resource.txt");
    
    //////////////////////////////////////////////////

    JobClient.runJob(job);
  }

}