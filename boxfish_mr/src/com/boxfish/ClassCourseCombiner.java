package com.boxfish;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.boxfish.util.Util;

public class ClassCourseCombiner extends ReducerBase {
	private Record classRecord;
	private Record courseRecord;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		classRecord = context.createMapOutputKeyRecord();
		courseRecord = context.createMapOutputValueRecord();
		
	}
	/**
	 * key:classe_id
	 * value:course_id
	 * outKey:class_id
	 * outVal:course_id,多个course_id用','分隔
	 */
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		if(StringUtils.isEmpty(key.getString(0))){
			return;
		}
		
		Set<String> courseSet = Util.getCourseSet(values);
		if(courseSet.isEmpty()){
			return;
		}
		classRecord.set(0, key.get(0));
		courseRecord.setString(0,Util.getCourseStr(courseSet));
		context.write(classRecord, courseRecord);
	}
	
	

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
