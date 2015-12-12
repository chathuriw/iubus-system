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

import fall2015.b565.iubus.db.DataReaderTime;
import fall2015.b565.iubus.utils.Constants;
import fall2015.b565.iubus.utils.IUBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class StopTimes {
    private static Logger log = LoggerFactory.getLogger(StopTimes.class);
    public static void main(String[] args) {
        calculateDwellTime();
        calculateBetweenStopsTime();
    }

    public static void calculateDwellTime(){
        PrintWriter aDwellTime, bDwellTime, eDwellTime, xDwellTime;
        try {
            String aFallDwellTimeQuery = IUBusUtils.generateAQueryDwellTime(Constants.TableNames.A_MR_FALL_ROUTE_TIME_TABLE);
            String aSpringDwellTimeQuery = IUBusUtils.generateAQueryDwellTime(Constants.TableNames.A_MR_SPRING_ROUTE_TIME_TABLE);
            String bFallDwellTimeQuery = IUBusUtils.generateBQueryDwellTime(Constants.TableNames.B_MR_FALL_ROUTE_TIME_TABLE);
            String bSpringDwellTimeQuery = IUBusUtils.generateBQueryDwellTime(Constants.TableNames.B_MR_SPRING_ROUTE_TIME_TABLE);
            String eFallDwellTimeQuery = IUBusUtils.generateEQueryDwellTime(Constants.TableNames.E_MR_FALL_ROUTE_TIME_TABLE);
            String eSpringDwellTimeQuery = IUBusUtils.generateEQueryDwellTime(Constants.TableNames.E_MR_SPRING_ROUTE_TIME_TABLE);
            String xFallDwellTimeQuery = IUBusUtils.generateXQueryDwellTime(Constants.TableNames.X_MR_FALL_ROUTE_TIME_TABLE);
            String xSpringDwellTimeQuery = IUBusUtils.generateXQueryDwellTime(Constants.TableNames.X_MR_SPRING_ROUTE_TIME_TABLE);

            String resultFileLocation = IUBusUtils.getResultFolder();
            String aDwellTimeFileName = resultFileLocation + IUBusUtils.getResultFileName("dwell_time_A") + ".csv";
            String bDwellTimeFileName = resultFileLocation + IUBusUtils.getResultFileName("dwell_time_B") + ".csv";
            String eDwellTimeFileName = resultFileLocation + IUBusUtils.getResultFileName("dwell_time_E") + ".csv";
            String xDwellTimeFileName = resultFileLocation + IUBusUtils.getResultFileName("dwell_time_X") + ".csv";

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()) {
                resultFolder.mkdir();
            }
            File aDwellTimeFile = new File(aDwellTimeFileName);
            File bDwellTimeFile = new File(bDwellTimeFileName);
            File eDwellTimeFile = new File(eDwellTimeFileName);
            File xDwellTimeFile = new File(xDwellTimeFileName);
            aDwellTime = new PrintWriter(aDwellTimeFile, "UTF-8");
            bDwellTime = new PrintWriter(bDwellTimeFile, "UTF-8");
            eDwellTime = new PrintWriter(eDwellTimeFile, "UTF-8");
            xDwellTime = new PrintWriter(xDwellTimeFile, "UTF-8");
            aDwellTime.println("stopid,avgTimeFall,avgTimeSpring");
            bDwellTime.println("stopid,avgTimeFall,avgTimeSpring");
            eDwellTime.println("stopid,avgTimeFall,avgTimeSpring");
            xDwellTime.println("stopid,avgTimeFall,avgTimeSpring");

            DataReaderTime dataReaderTime = new DataReaderTime();
            Map<String, Double> avgDwelTimesAFall = dataReaderTime.getAvgDwelTimes(aFallDwellTimeQuery);
            Map<String, Double> avgDwelTimesASpring = dataReaderTime.getAvgDwelTimes(aSpringDwellTimeQuery);
            for (String  columnName : avgDwelTimesAFall.keySet()){
                aDwellTime.println(columnName + "," + avgDwelTimesAFall.get(columnName) + "," + avgDwelTimesASpring.get(columnName));
            }

            Map<String, Double> avgDwelTimesBFall = dataReaderTime.getAvgDwelTimes(bFallDwellTimeQuery);
            Map<String, Double> avgDwelTimesBSpring = dataReaderTime.getAvgDwelTimes(bSpringDwellTimeQuery);
            for (String  columnName : avgDwelTimesBFall.keySet()){
                bDwellTime.println(columnName + "," + avgDwelTimesBFall.get(columnName) + "," + avgDwelTimesBSpring.get(columnName));
            }

            Map<String, Double> avgDwelTimesEFall = dataReaderTime.getAvgDwelTimes(eFallDwellTimeQuery);
            Map<String, Double> avgDwelTimesESpring = dataReaderTime.getAvgDwelTimes(eSpringDwellTimeQuery);
            for (String  columnName : avgDwelTimesEFall.keySet()){
                eDwellTime.println(columnName + "," + avgDwelTimesEFall.get(columnName) + "," + avgDwelTimesESpring.get(columnName));
            }

            Map<String, Double> avgDwelTimesXFall = dataReaderTime.getAvgDwelTimes(xFallDwellTimeQuery);
            Map<String, Double> avgDwelTimesXSpring = dataReaderTime.getAvgDwelTimes(xSpringDwellTimeQuery);
            for (String  columnName : avgDwelTimesXFall.keySet()){
                xDwellTime.println(columnName + "," + avgDwelTimesXFall.get(columnName) + "," + avgDwelTimesXSpring.get(columnName));
            }

            aDwellTime.flush();
            bDwellTime.flush();
            eDwellTime.flush();
            xDwellTime.flush();
        } catch (FileNotFoundException e) {
            log.error("Error while retrieving data", e);
            throw new RuntimeException("Error while retrieving data", e);
        } catch (UnsupportedEncodingException e) {
            log.error("Error while retrieving data", e);
            throw new RuntimeException("Error while retrieving data", e);
        } catch (Exception e) {
            log.error("Error while retrieving data", e);
            throw new RuntimeException("Error while retrieving data", e);
        }
    }

    public static void calculateBetweenStopsTime(){
        PrintWriter aBetweenStopsTime, bBetweenStopsTime, eBetweenStopsTime, xBetweenStopsTime;
        try {
            String aFallBetweenStopsQuery = IUBusUtils.generateAQueryBetweenStops(Constants.TableNames.A_MR_FALL_ROUTE_TIME_TABLE);
            String aSpringBetweenStopsQuery = IUBusUtils.generateAQueryBetweenStops(Constants.TableNames.A_MR_SPRING_ROUTE_TIME_TABLE);
            String bFallBetweenStopsQuery = IUBusUtils.generateBQueryBetweenStops(Constants.TableNames.B_MR_FALL_ROUTE_TIME_TABLE);
            String bSpringBetweenStopsQuery = IUBusUtils.generateBQueryBetweenStops(Constants.TableNames.B_MR_SPRING_ROUTE_TIME_TABLE);
            String eFallBetweenStopsQuery = IUBusUtils.generateEQueryBetweenStops(Constants.TableNames.E_MR_FALL_ROUTE_TIME_TABLE);
            String eSpringBetweenStopsQuery = IUBusUtils.generateEQueryBetweenStops(Constants.TableNames.E_MR_SPRING_ROUTE_TIME_TABLE);
            String xFallBetweenStopsQuery = IUBusUtils.generateXQueryBetweenStops(Constants.TableNames.X_MR_FALL_ROUTE_TIME_TABLE);
            String xSpringBetweenStopsQuery = IUBusUtils.generateXQueryBetweenStops(Constants.TableNames.X_MR_SPRING_ROUTE_TIME_TABLE);

            String resultFileLocation = IUBusUtils.getResultFolder();
            String aBetweenStopsFileName = resultFileLocation + IUBusUtils.getResultFileName("between_stops_time_A") + ".csv";
            String bBetweenStopsFileName = resultFileLocation + IUBusUtils.getResultFileName("between_stops_time_B") + ".csv";
            String eBetweenStopsFileName = resultFileLocation + IUBusUtils.getResultFileName("between_stops_time_E") + ".csv";
            String xBetweenStopsFileName = resultFileLocation + IUBusUtils.getResultFileName("between_stops_time_X") + ".csv";

            File resultFolder = new File(resultFileLocation);
            if (!resultFolder.exists()) {
                resultFolder.mkdir();
            }
            File aBetweenStopsFile = new File(aBetweenStopsFileName);
            File bBetweenStopsFile = new File(bBetweenStopsFileName);
            File eBetweenStopsFile = new File(eBetweenStopsFileName);
            File xBetweenStopsFile = new File(xBetweenStopsFileName);
            aBetweenStopsTime = new PrintWriter(aBetweenStopsFile, "UTF-8");
            bBetweenStopsTime = new PrintWriter(bBetweenStopsFile, "UTF-8");
            eBetweenStopsTime = new PrintWriter(eBetweenStopsFile, "UTF-8");
            xBetweenStopsTime = new PrintWriter(xBetweenStopsFile, "UTF-8");
            aBetweenStopsTime.println("stopid,avgTimeFall,avgTimeSpring");
            bBetweenStopsTime.println("stopid,avgTimeFall,avgTimeSpring");
            eBetweenStopsTime.println("stopid,avgTimeFall,avgTimeSpring");
            xBetweenStopsTime.println("stopid,avgTimeFall,avgTimeSpring");

            DataReaderTime dataReaderTime = new DataReaderTime();
            Map<String, Double> avgBetweenStopsAFall = dataReaderTime.getAvgTimesBetweenStops(aFallBetweenStopsQuery);
            Map<String, Double> avgBetweenStopASpring = dataReaderTime.getAvgTimesBetweenStops(aSpringBetweenStopsQuery);
            for (String  columnName : avgBetweenStopsAFall.keySet()){
                aBetweenStopsTime.println(columnName + "," + avgBetweenStopsAFall.get(columnName) + "," + avgBetweenStopASpring.get(columnName));
            }

            Map<String, Double> avgBetweenStopBFall = dataReaderTime.getAvgTimesBetweenStops(bFallBetweenStopsQuery);
            Map<String, Double> avgBetweenStopBSpring = dataReaderTime.getAvgTimesBetweenStops(bSpringBetweenStopsQuery);
            for (String  columnName : avgBetweenStopBFall.keySet()){
                bBetweenStopsTime.println(columnName + "," + avgBetweenStopBFall.get(columnName) + "," + avgBetweenStopBSpring.get(columnName));
            }

            Map<String, Double> avgBetweenStopEFall = dataReaderTime.getAvgTimesBetweenStops(eFallBetweenStopsQuery);
            Map<String, Double> avgBetweenStopESpring = dataReaderTime.getAvgTimesBetweenStops(eSpringBetweenStopsQuery);
            for (String  columnName : avgBetweenStopEFall.keySet()){
                eBetweenStopsTime.println(columnName + "," + avgBetweenStopEFall.get(columnName) + "," + avgBetweenStopESpring.get(columnName));
            }

            Map<String, Double> avgBetweenStopXFall = dataReaderTime.getAvgTimesBetweenStops(xFallBetweenStopsQuery);
            Map<String, Double> avgBetweenStopXSpring = dataReaderTime.getAvgTimesBetweenStops(xSpringBetweenStopsQuery);
            for (String  columnName : avgBetweenStopXFall.keySet()){
                xBetweenStopsTime.println(columnName + "," + avgBetweenStopXFall.get(columnName) + "," + avgBetweenStopXSpring.get(columnName));
            }

            aBetweenStopsTime.flush();
            bBetweenStopsTime.flush();
            eBetweenStopsTime.flush();
            xBetweenStopsTime.flush();
        } catch (FileNotFoundException e) {
            log.error("Error while retrieving data", e);
            throw new RuntimeException("Error while retrieving data", e);
        } catch (UnsupportedEncodingException e) {
            log.error("Error while retrieving data", e);
            throw new RuntimeException("Error while retrieving data", e);
        } catch (Exception e) {
            log.error("Error while retrieving data", e);
            throw new RuntimeException("Error while retrieving data", e);
        }
    }

}
