package com.boxfish;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.boxfish.log.LoggerFactory;

public class ClassCourseDriver {

	public static void main(String[] args) throws OdpsException {

		if (args.length != 5) {
			System.err.println("Usage:");
			System.err.println("\t input:course_learn_event");
			System.err.println("\t output:class_course_statis_mr");
			System.err.println("\t resource_file");
			System.err.println("\t last_str");
			System.err.println("\t doneDirPath");
			System.exit(1);
		}
		String courseLearnIn = args[0];
		String classCourseOut = args[1];
		String lastStr = args[2];
		String userClassDictFile = args[3];
		String doneDirPath = args[4];
		LoggerFactory.syslogger.info(String.format("courseLearnIn = %s,classCourseOut=%s,lastStr=%s,userClassDictFile=%s,doneDirPath=%s",courseLearnIn,classCourseOut,lastStr,userClassDictFile,doneDirPath));
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("class_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("course_id:string"));
		// TODO: specify input and output tables
		
		LoggerFactory.syslogger.info("lastStr:" + lastStr);
		LinkedHashMap<String, String> input = new LinkedHashMap<String, String>();
		input.put("date_time", lastStr);
		InputUtils.addTable(TableInfo.builder().tableName(courseLearnIn)
				.partSpec(input).build(), job);

		OutputUtils.addTable(TableInfo.builder().tableName(classCourseOut)
				.partSpec(input).build(), job);
		job.set("resourceName", userClassDictFile);
		////////////////在线环境删除该行代码///////////////////
	   
	    //job.setResources(userClassDictFile);
	    
	    //////////////////////////////////////////////////
		
		job.setMapperClass(ClassCourseMapper.class);
		job.setCombinerClass(ClassCourseCombiner.class);
		job.setReducerClass(ClassCourseReducer.class);

		RunningJob rj = JobClient.runJob(job);
		LoggerFactory.syslogger.info("mr instanceId:" + rj.getInstanceID());

		while (!rj.isComplete()) {
			LoggerFactory.syslogger.info("job status:" + rj.getJobStatus());
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LoggerFactory.syslogger.error("run mr job fail", e);
			}
		}
		LoggerFactory.syslogger.info("mr is successful:" + rj.isSuccessful());
		File doneDir = new File(doneDirPath);
		if(!doneDir.exists()){
			doneDir.mkdirs();
		}
		
		try {
			File doneFile = new File(doneDir, lastStr + ".done");
			doneFile.createNewFile();
		} catch (IOException e) {
			LoggerFactory.syslogger.info("create mr done fail",e);
		}
	}

}
