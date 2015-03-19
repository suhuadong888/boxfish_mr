package com.boxfish;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.boxfish.log.LoggerFactory;
/**
 * 日期对应的分区表
 * @author mac
 *
 */
public class ClassCourseMapper extends MapperBase {
	//班级编号
	private Record classIdRecord;
	//课程
	private Record courseRecord;
	/**
	 * userId对应的班级字典
	 */
	private Map<String, Set<String>> userIdClassIdMap;
	
	private void initUserClassMap(TaskContext context){
		userIdClassIdMap = new HashMap<String, Set<String>>();
		String fileName = context.getJobConf().get("resourceName");
		System.out.println("fileName:" + fileName);
		try {
			BufferedInputStream bin = context.readResourceFileAsStream(fileName);
			System.out.println("read resource success");
			BufferedReader breader = new BufferedReader(new InputStreamReader(bin,"utf-8"));
			String line = null;
			while((line = breader.readLine()) != null){
				LoggerFactory.syslogger.info(line);
				if(StringUtils.isEmpty(line)){
					continue;
				}
				String cols[] = line.split("\\t");
				if(cols == null || cols.length < 2){
					continue;
				}
				String userId = cols[0];
				String classId = cols[1];
				if(userIdClassIdMap.get(userId) == null){
					Set<String> classIdSet = new HashSet<String>();
					classIdSet.add(classId);
					userIdClassIdMap.put(userId, classIdSet);
					
				}else{
					userIdClassIdMap.get(userId).add(classId);
				}
			}
		} catch (IOException e) {
			System.out.println("read resouce fail" + e.toString());
			System.exit(4);
		}
	}
	
	
	@Override
	public void setup(TaskContext context) throws IOException {
		classIdRecord = context.createMapOutputKeyRecord();
		courseRecord =  context.createMapOutputValueRecord();
		//初始userId到classId的字典
		initUserClassMap(context);
	}
	
	/**
	 *  outputKey(class_id:string)
	 *  outputKey(course_id:string)
	 */
	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		LoggerFactory.syslogger.info("start map");
		//userId
		String userId = record.getString("user_id");
		if(StringUtils.isEmpty(userId)){
			return;
		}
		//班级
		Set<String> classIdSet = this.userIdClassIdMap.get(userId);
		if(classIdSet == null || classIdSet.isEmpty()){
			return;
		}
		//课程编号
		String courseId = record.getString("course_id");
		if(StringUtils.isEmpty(courseId)){
			return;
		}
		
		Iterator<String> classIter = classIdSet.iterator();
		while(classIter.hasNext()){
			String classId = classIter.next();
			if(StringUtils.isEmpty(classId)){
				continue;
			}
			classIdRecord.setString("class_id", classId);
			courseRecord.setString("course_id", courseId);
			context.write(classIdRecord, courseRecord);
		}
		
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}
	
}
