package com.boxfish;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.boxfish.util.Util;

public class ClassCourseReducer extends ReducerBase {
	private Record result;
	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	/**
	 * key:class_id
	 * val:course_id,多个course_id用','分隔
	 * outVal:result(date_time:string,class_id:string,course_id:string(多个course_id用','分隔))
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
		result.setString("class_id", key.getString(0));
		result.setString("course_id", Util.getCourseStr(courseSet));
		context.write(result);
	}

	
	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
