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

import fall2015.b565.iubus.db.DataReaderARoute;
import fall2015.b565.iubus.db.DataReaderBRoute;
import fall2015.b565.iubus.db.DataReaderERoute;
import fall2015.b565.iubus.db.DataReaderXRoute;
import fall2015.b565.iubus.utils.IUBusUtils;
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
        calculateARouteVarianceFall();
        calculateARouteVarianceSpring();
        calculateBRouteVarianceFall();
        calculateBRouteVarianceSpring();
        calculateERouteVarianceFall();
        calculateERouteVarianceSpring();
        calculateXRouteVarianceFall();
        calculateXRouteVarianceSpring();
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
//            Schedule aShedule = reader.getASheduleFall(aRouteMR);
//            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
//            List<ActualSchedule> actualASheduleList = reader.getActualBSheduleFall(aRouteMR);
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


    public static void calculateARouteVarianceFall() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("fall_A");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("fall_A");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderARoute reader = new DataReaderARoute();
            int aRouteMR = 331;
            Schedule aShedule = reader.getASheduleFall(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualASheduleFall(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesFall();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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

    public static void calculateARouteVarianceSpring() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("spring_A");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("spring_A");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderARoute reader = new DataReaderARoute();
            int aRouteMR = 354;
            Schedule aShedule = reader.getASheduleSpring(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualASheduleSpring(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesSpring();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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

    public static void calculateBRouteVarianceFall() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("fall_B");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("fall_B");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderBRoute reader = new DataReaderBRoute();
            int bRouteMR = 318;
            Schedule bSheduleFall = reader.getBSheduleFall(bRouteMR);
            Map<Time, Time[]> allASchedules = bSheduleFall.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualBSheduleFall(bRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesFall();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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

    public static void calculateBRouteVarianceSpring() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("spring_B");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("spring_B");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderBRoute reader = new DataReaderBRoute();
            int bRouteMR = 357;
            Schedule aShedule = reader.getBScheduleSpring(bRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualBScheduleSpring(bRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesSpring();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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


    public static void calculateERouteVarianceFall() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("fall_E");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("fall_E");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderERoute reader = new DataReaderERoute();
            int aRouteMR = 320;
            Schedule aShedule = reader.getESheduleFall(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualESheduleFall(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesFall();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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

    public static void calculateERouteVarianceSpring() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("spring_E");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("spring_E");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderERoute reader = new DataReaderERoute();
            int aRouteMR = 361;
            Schedule aShedule = reader.getEScheduleSpring(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualEScheduleSpring(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesSpring();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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


    public static void calculateXRouteVarianceFall() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("fall_X");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("fall_X");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderXRoute reader = new DataReaderXRoute();
            int aRouteMR = 325;
            Schedule aShedule = reader.getXSheduleFall(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualXSheduleFall(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesFall();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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

    public static void calculateXRouteVarianceSpring() throws Exception{
        PrintWriter varienceResultWriter, varianceAtTimeWriter;
        Map<Time, Map<Date, Double>> varianceTimeMap = new HashMap<Time, Map<Date, Double>>();
        try {
            String resultFileLocation = IUBusUtils.getResultFolder();
            String varianceResultFileName = resultFileLocation + getVarianceResultFileName("spring_X");
            String varianceAtTimeFileName = resultFileLocation + getVariancAtTimeeResultFileName("spring_X");

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()){
                resultFolder.mkdir();
            }
            File varResultFile = new File(varianceResultFileName);
            File varAtTimeResultFile = new File(varianceAtTimeFileName);
            varienceResultWriter = new PrintWriter(varResultFile, "UTF-8");
            varianceAtTimeWriter = new PrintWriter(varAtTimeResultFile, "UTF-8");
            varienceResultWriter.println("Date, Start Time Schedule, Start Time Actual, Variance ");
            DataReaderXRoute reader = new DataReaderXRoute();
            int aRouteMR = 364;
            Schedule aShedule = reader.getXScheduleSpring(aRouteMR);
            Map<Time, Time[]> allASchedules = aShedule.getAllSchedules();
            List<ActualSchedule> actualASheduleList = reader.getActualXScheduleSpring(aRouteMR);
            List<Date> distinctDates = reader.getDistinctDatesSpring();

            for (Time startA : allASchedules.keySet()){
                Map<Date, Double> varianceDateMap = new HashMap<Date, Double>();
                for (ActualSchedule actualSchedule : actualASheduleList){
                    Map<Time, Time[]> allSchedules = actualSchedule.getAllSchedules();
                    for (Time startTime : allSchedules.keySet()){
                        if (startTime != null && startTime.getTime() != 0 ){
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


    private static String getVarianceResultFileName(String prefix){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss");
        Calendar cal = Calendar.getInstance();
        String date = dateFormat.format(cal.getTime());
        return "variance_" + prefix + "_" + date;
    }

    private static String getVariancAtTimeeResultFileName(String prefix){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss");
        Calendar cal = Calendar.getInstance();
        String date = dateFormat.format(cal.getTime());
        return "variance_time_" + prefix + "_" + date;
    }
}
