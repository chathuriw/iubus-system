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
import fall2015.b565.iubus.utils.IuBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Date;
import java.sql.Time;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScheduleVarience {
    private static Logger log = LoggerFactory.getLogger(ScheduleVarience.class);
    public static void main(String[] args) throws Exception{
        calculateVarianceTime();
//        PrintWriter varienceResultWriter;
//        try {
//            String resultFileLocation = IuBusUtils.getResultFolder();
//            String varianceResultFileName = resultFileLocation + getVarianceResultFileName();
//
//            File resultFolder = new File(resultFileLocation);
//            if (!resultFolder.exists()){
//                resultFolder.mkdir();
//            }
//            File varResultFile = new File(varianceResultFileName);
//            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
//            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
//            DataReader reader = new DataReader();
//            int aRouteMR = 331;
//            Schedule aShedule = reader.getAShedule(aRouteMR);
//            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
//            List<ActualSchedule> actualASheduleList = reader.getActualAShedule(aRouteMR);
//            for (ActualSchedule actualSchedule : actualASheduleList){
//                Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
//                for (Time startTime : allSchedules.keySet()){
//                    for (Time startA : allASchedules.keySet()){
//                        if (startTime.getTime() != 0 ){
//                            if (startA.getTime() <= (startTime.getTime() + 5*1000*60) && startA.getTime() >= (startTime.getTime() - 10*1000*60)){
//                                double variance = (startTime.getTime() - startA.getTime())/(1000.0*60);
//                                varienceResultWriter.println(actualSchedule.getDate() + "," + startA + "," + startTime + "," + variance);
//                            }
//                        }
//                    }
//                }
//            }
//            varienceResultWriter.flush();
//        } catch (ParseException e) {
//            log.error("Error while parsing date", e);
//            throw new RuntimeException("Error while parsing date", e);
//        }
    }


    public static void calculateVarianceTime() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IuBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName();
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName();

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReader reader = new DataReader();
            int aRouteMR = 331;
            Schedule aShedule = reader.getAShedule(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualAShedule(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDates();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime.getTime() != 0 ){
                            if (startA.getTime() <= (startTime.getTime() + 5*1000*60) && startA.getTime() >= (startTime.getTime() - 10*1000*60)){
                                double variance = (startTime.getTime() - startA.getTime())/(1000.0*60);
                                varienceResultWriter.println(actualSchedule.getDate() + "," + startA + "," + startTime + "," + variance);
                                for (Date date : distinctDates){
                                    if (date.equals(actualSchedule.getDate())){
                                        varianceDateMap.put(date, variance);
                                    }
                                }
                            }
                        }
                    }
                }
                varianceTimeMap.put(startA, varianceDateMap);
            }

            // write data to other result file
            for (Date distinctDate : distinctDates) {
                for (Time startTime : varianceTimeMap.keySet()) {
                    Map<Date, Double> dateMap = varianceTimeMap.get(startTime);
                    Double variance = dateMap.get(distinctDate);
                    varianceAtTimeWriter.println(startTime + "," + distinctDate + "," + variance);
                }
            }
            varienceResultWriter.flush();
            varianceAtTimeWriter.flush();
            System.out.println(varianceTimeMap.size());
        } catch (ParseException e) {
            log.error("Error while parsing date", e);
            throw new RuntimeException("Error while parsing date", e);
        }
    }

    private static String getVarianceResultFileName(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss");
        Calendar cal = Calendar.getInstance();
        String date = dateFormat.format(cal.getTime());
        return "variance_" + date;
    }

    private static String getVariancAtTimeeResultFileName(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss");
        Calendar cal = Calendar.getInstance();
        String date = dateFormat.format(cal.getTime());
        return "variance_time_" + date;
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
