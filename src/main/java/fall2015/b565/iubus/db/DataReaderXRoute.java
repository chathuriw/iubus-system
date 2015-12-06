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

package fall2015.b565.iubus.db;

import fall2015.b565.iubus.ActualSchedule;
import fall2015.b565.iubus.Schedule;
import fall2015.b565.iubus.utils.Constants;
import fall2015.b565.iubus.utils.IUBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataReaderXRoute {
    private static Logger log = LoggerFactory.getLogger(DataReaderXRoute.class);

    public Schedule getXSheduleFall(int routeId) throws Exception{
        Schedule schedule = new Schedule(routeId);
        Map<Time, Time[]> allSchedules = new HashMap<Time, Time[]>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.X_MR_FALL_SCHEDULE_TABLE ;
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Time startTime = resultSet.getTime(1);
                    Time[] restOfTheTime = {resultSet.getTime(2), resultSet.getTime(3)};
                    allSchedules.put(startTime, restOfTheTime);
                }
                schedule.setAllSchedules(allSchedules);
            }
            return schedule;
        } catch (SQLException e) {
            String error = "Error while retrieving data from database.";
            log.error(error, e);
            throw new Exception(error, e);
        }  finally {
            try{
                if (preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                log.error("Error while connecting with mysql server", e);
                throw new Exception("Error while connecting with mysql server", e);
            }
        }
    }

    public List<ActualSchedule> getActualXSheduleFall(int routeId) throws Exception{
        List<ActualSchedule> allActualSchedules = new ArrayList<ActualSchedule>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT e.day, e.busid, e.from76, e.to64, e.to76 FROM " + Constants.TableNames.X_MR_FALL_ROUTE_TABLE  + " e ORDER BY e.day, e.from76";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Date date = resultSet.getDate(1);
                    ActualSchedule actualSchedule = new ActualSchedule(routeId, date);
                    Map<Time, Time[]> allSchedules = new HashMap<Time, Time[]>();
                    Time startTime = resultSet.getTime(3);
                    Time[] restOfTheTime = {resultSet.getTime(4), resultSet.getTime(5)};
                    allSchedules.put(startTime, restOfTheTime);
                    actualSchedule.setAllSchedules(allSchedules);
                    allActualSchedules.add(actualSchedule);
                }
            }
            return allActualSchedules;
        } catch (SQLException e) {
            String error = "Error while retrieving data from database.";
            log.error(error, e);
            throw new Exception(error, e);
        }  finally {
            try{
                if (preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                log.error("Error while connecting with mysql server", e);
                throw new Exception("Error while connecting with mysql server", e);
            }
        }
    }

    public List<Date> getDistinctDatesFall() throws Exception{
        List<Date> distinctDates = new ArrayList<Date>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT DISTINCT(e.day) FROM " + Constants.TableNames.X_MR_FALL_ROUTE_TABLE +" e ORDER BY e.day ASC";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    distinctDates.add(resultSet.getDate(1));
                }
            }
            return distinctDates;
        } catch (SQLException e) {
            String error = "Error while retrieving data from database.";
            log.error(error, e);
            throw new Exception(error, e);
        }  finally {
            try{
                if (preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                log.error("Error while connecting with mysql server", e);
                throw new Exception("Error while connecting with mysql server", e);
            }
        }
    }

    public Schedule getXScheduleSpring(int routeId) throws Exception{
        Schedule schedule = new Schedule(routeId);
        Map<Time, Time[]> allSchedules = new HashMap<Time, Time[]>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.X_MR_SPRING_SCHEDULE_TABLE;
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Time startTime = resultSet.getTime(1);
                    Time[] restOfTheTime = {resultSet.getTime(2), resultSet.getTime(3)};
                    allSchedules.put(startTime, restOfTheTime);
                }
                schedule.setAllSchedules(allSchedules);
            }
            return schedule;
        } catch (SQLException e) {
            String error = "Error while retrieving data from database.";
            log.error(error, e);
            throw new Exception(error, e);
        }  finally {
            try{
                if (preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                log.error("Error while connecting with mysql server", e);
                throw new Exception("Error while connecting with mysql server", e);
            }
        }
    }

    public List<ActualSchedule> getActualXScheduleSpring(int routeId) throws Exception{
        List<ActualSchedule> allActualSchedules = new ArrayList<ActualSchedule>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT e.day, e.busid, e.from76, e.to64, e.to76 FROM " + Constants.TableNames.X_MR_SPRING_ROUTE_TABLE + " e ORDER BY e.day, e.from76";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Date date = resultSet.getDate(1);
                    ActualSchedule actualSchedule = new ActualSchedule(routeId, date);
                    Map<Time, Time[]> allSchedules = new HashMap<Time, Time[]>();
                    Time startTime = resultSet.getTime(3);
                    Time[] restOfTheTime = {resultSet.getTime(4), resultSet.getTime(5)};
                    allSchedules.put(startTime, restOfTheTime);
                    actualSchedule.setAllSchedules(allSchedules);
                    allActualSchedules.add(actualSchedule);
                }
            }
            return allActualSchedules;
        } catch (SQLException e) {
            String error = "Error while retrieving data from database.";
            log.error(error, e);
            throw new Exception(error, e);
        }  finally {
            try{
                if (preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                log.error("Error while connecting with mysql server", e);
                throw new Exception("Error while connecting with mysql server", e);
            }
        }
    }

    public List<Date> getDistinctDatesSpring() throws Exception{
        List<Date> distinctDates = new ArrayList<Date>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT DISTINCT(e.day) FROM " + Constants.TableNames.X_MR_SPRING_ROUTE_TABLE +  " e ORDER BY e.day ASC";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    distinctDates.add(resultSet.getDate(1));
                }
            }
            return distinctDates;
        } catch (SQLException e) {
            String error = "Error while retrieving data from database.";
            log.error(error, e);
            throw new Exception(error, e);
        }  finally {
            try{
                if (preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                log.error("Error while connecting with mysql server", e);
                throw new Exception("Error while connecting with mysql server", e);
            }
        }
    }
}
