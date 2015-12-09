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

import fall2015.b565.iubus.Weather;
import fall2015.b565.iubus.utils.Constants;
import fall2015.b565.iubus.utils.IUBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DataReaderWeather {
    private static Logger log = LoggerFactory.getLogger(DataReaderWeather.class);
    public Weather getFallWeatherForDate(Date date) throws Exception{
        Weather weather = new Weather();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;

        String queryString = "SELECT * FROM " + Constants.TableNames.FALL_WEATHER_TABLE + " e where e.date = '" + date + "'";
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    weather.setDate(resultSet.getDate(2));
                    weather.setDay(resultSet.getString(1));
                    weather.setPrecipitation(resultSet.getFloat(9));
                    weather.setTotalRidership(resultSet.getInt(3));
                }
            }
            return weather;
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

    public Map<Date, Weather> getAllFallWeather() throws Exception{
        Map<Date, Weather> weatherMap = new HashMap<Date, Weather>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;

        String queryString = "SELECT * FROM " + Constants.TableNames.FALL_WEATHER_TABLE;
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Weather weather = new Weather();
                    Date date = resultSet.getDate(2);
                    weather.setDate(date);
                    weather.setDay(resultSet.getString(1));
                    weather.setPrecipitation(resultSet.getFloat(9));
                    weather.setTotalRidership(resultSet.getInt(3));
                    weatherMap.put(date, weather);
                }
            }
            return weatherMap;
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

    public Map<Date, Weather> getAllSpringWeather() throws Exception{
        Map<Date, Weather> weatherMap = new HashMap<Date, Weather>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;

        String queryString = "SELECT * FROM " + Constants.TableNames.SPRING_WEATHER_TABLE;
//        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    Weather weather = new Weather();
                    Date date = resultSet.getDate(2);
                    weather.setDate(date);
                    weather.setDay(resultSet.getString(1));
                    weather.setPrecipitation(resultSet.getFloat(9));
                    weather.setTotalRidership(resultSet.getInt(3));
                    weatherMap.put(date, weather);
                }
            }
            return weatherMap;
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
