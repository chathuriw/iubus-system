/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

package fall2015.b565.iubus;

import fall2015.b565.iubus.db.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScheduleVarience {
    private static Logger log = LoggerFactory.getLogger(ScheduleVarience.class);
    public static void main(String[] args) throws Exception{
        try {
            DataReader reader = new DataReader();
            int aRouteMR = 331;
            int count = 0;
            Schedule aShedule = reader.getAShedule(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualAShedule(aRouteMR);
            for (ActualSchedule actualSchedule : actualASheduleList){
                Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                for (Time startTime : allSchedules.keySet()){
                    for (Time startA : allASchedules.keySet()){
                        if (startTime.getTime() != 0 ){
                            if (startA.getTime() <= (startTime.getTime() + 10*1000*60) && startA.getTime() >= (startTime.getTime() - 5*1000*60)){
                                long variance = startTime.getTime() - startA.getTime();
                                System.out.println("Varience : " + variance);
                                System.out.println("Start Time Actual : " + startTime);
                                System.out.println("Start Time Schedule : " + startA);
                                System.out.println("Date : " + actualSchedule.getDate());
                                count++;
                            }
                        }
                    }
                }
            }
            System.out.println(count);
        } catch (ParseException e) {
            log.error("Error while parsing date", e);
            throw new RuntimeException("Error while parsing date", e);
        }
    }

//    private static long getMean(Set<Time> timeSet){
//        long accumilatedTime = 0;
//        for (Time time : timeSet){
//            accumilatedTime += time.getTime();
//        }
//        long mean = accumilatedTime / timeSet.size();
//        System.out.println("Mean : " + new Time(mean));
//
//        return mean;
//    }
//
//    private static double getVariance(Set<Time> timeSet) {
//        long temp = 0;
//        long mean = getMean(timeSet);
//        for (Time time : timeSet){
//            temp += (mean - time.getTime()) * (mean - time.getTime());
//        }
//        return temp/timeSet.size();
//    }
}
