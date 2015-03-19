package com.boxfish.util;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.aliyun.odps.data.Record;

public class Util {
	public static  Set<String> getCourseSet(Iterator<Record> values){
		Set<String> courseSet = new HashSet<String>();
		while(values.hasNext()){
			String courseId = values.next().getString(0);
			if(StringUtils.isEmpty(courseId)){
				continue;
			}
			courseSet.add(courseId);
		}
		return courseSet;
	}
	
	public static String getCourseStr(Set<String> courseSet){
		StringBuffer courseSbf = new StringBuffer();
		int courseLen = courseSet.size();
		int courseIndex = 0;
		Iterator<String> courseIter = courseSet.iterator();
		while(courseIter.hasNext()){
			String course = courseIter.next();
			if(courseIndex == (courseLen -1)){
				courseSbf.append(course);
			}else{
				courseSbf.append(course).append(",");
			}
			courseIndex++;
		}
		return courseSbf.toString();
	}
	
	public static String getLastStr(){
		Date lastDate = DateUtils.truncate(new Date(),Calendar.DAY_OF_MONTH);
		lastDate = DateUtils.addDays(lastDate, -1);
		return DateFormatUtils.format(lastDate, "yyyy-MM-dd");
	}
}
