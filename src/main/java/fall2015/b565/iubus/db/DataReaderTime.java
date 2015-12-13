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

import fall2015.b565.iubus.utils.IUBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DataReaderTime {
    private static Logger log = LoggerFactory.getLogger(DataReaderARoute.class);

    public Map<String, Double> getAvgDwelTimes(String queryString) throws Exception{
        Map<String, Double> avgDwellTimes = new HashMap<String, Double>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    for (int i =1; i <= columnCount; i++){
                        avgDwellTimes.put(rsmd.getColumnName(i), resultSet.getDouble(i));
                    }
                }
            }
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
        return avgDwellTimes;
    }

    public Map<String, Double> getAvgTimesBetweenStops(String queryString) throws Exception{
        Map<String, Double> avgDwellTimes = new HashMap<String, Double>();
        String connectionURL =  IUBusUtils.getJDBCUrl();
        Connection connection = null;
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, IUBusUtils.getJDBCUser(), IUBusUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    for (int i =1; i <= columnCount; i++){
                        avgDwellTimes.put(rsmd.getColumnName(i), resultSet.getDouble(i));
                    }
                }
            }
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
        return avgDwellTimes;
    }

}
