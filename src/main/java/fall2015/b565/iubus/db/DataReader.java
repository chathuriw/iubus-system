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

import fall2015.b565.iubus.utils.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DataReader {
    private static Logger log = LoggerFactory.getLogger(DataReader.class);

    public Map<Time, Time> getTime (Time btweenTime1, Time btweenTime2, Time scheduleTime, int routeId, int stopId) throws Exception{
        Map<Time, Time> timeMap = new HashMap<Time, Time>();
        String connectionURL =  DBUtils.getJDBCUrl();
        Connection connection = null;
        String queryString = "SELECT e.WHEN_NEW FROM TEST4 e WHERE  e.WHEN_NEW > '" + btweenTime1 + "' AND e.WHEN_NEW < '" + btweenTime2 + "' AND e.ROUTE_ID=" + routeId  + " AND e.FROM=" + stopId;
        System.out.println(queryString);
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(connectionURL, DBUtils.getJDBCUser(), DBUtils.getJDBCPWD());
            preparedStatement = connection.prepareStatement(queryString);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet != null){
                while (resultSet.next()) {
                    timeMap.put(resultSet.getTime(1), scheduleTime);
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
        return timeMap;
    }
}
