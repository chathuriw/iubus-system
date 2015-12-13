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

import fall2015.b565.iubus.Ridership;
import fall2015.b565.iubus.utils.Constants;
import fall2015.b565.iubus.utils.IUBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataReaderRidership {
    private static Logger log = LoggerFactory.getLogger(DataReaderRidership.class);

    public List<Time> getASheduleStartTimeFall(int routeId) throws Exception{
        List<Time> startTimeAFallList = new ArrayList<Time>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT e.from67or68 FROM " + Constants.TableNames.A_MR_FALL_SCHEDULE_TABLE + " e";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Time startTime = resultSet.getTime(1);
                    startTimeAFallList.add(startTime);
                }
            }
            return startTimeAFallList;
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

    public List<Ridership> getFallMRRiderships() throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.A_MR_FALL_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6;";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Date date = resultSet.getDate(1);
                    Ridership ridership = new Ridership();
                    ridership.setDate(date);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public Ridership getFallMRRidershipForGivenStartTimeDate(Time startTime, Date date) throws Exception{
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.A_MR_FALL_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "' AND e.date = '" + date + "'";
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            Ridership ridership = new Ridership();
            if (resultSet != null){
                while (resultSet.next()) {
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                }
            }
            return ridership;
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

    public List<Ridership> getAFallMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.A_MR_FALL_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public List<Ridership> getASpringMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.A_MR_SPRING_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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


    public List<Ridership> getBFallMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.B_MR_FALL_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public List<Ridership> getBSpringMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.B_MR_SPRING_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public List<Ridership> getEFallMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.E_MR_FALL_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public List<Ridership> getESpringMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.E_MR_SPRING_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public List<Ridership> getXFallMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.X_MR_FALL_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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

    public List<Ridership> getXSpringMRRidershipForGivenStartTime(Time startTime) throws Exception{
        List<Ridership> ridershipArrayList = new ArrayList<Ridership>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT * FROM " + Constants.TableNames.X_MR_SPRING_RIDERSHIP_TABLE + " e where e.dayOfWeek > 1 and e.dayOfWeek < 6  AND e.startingTime = '" + startTime + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Ridership ridership = new Ridership();
                    Date resultSetDate = resultSet.getDate(1);
                    ridership.setDate(resultSetDate);
                    ridership.setDayOfWeek(resultSet.getInt(2));
                    ridership.setScheduleName(resultSet.getString(3));
                    ridership.setStartTime(resultSet.getTime(4));
                    ridership.setInbound(resultSet.getInt(5));
                    ridership.setOutbound(resultSet.getInt(6));
                    ridership.setTotal(resultSet.getInt(7));
                    ridership.setBusId(resultSet.getInt(8));
                    ridershipArrayList.add(ridership);
                }
            }
            return ridershipArrayList;
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
