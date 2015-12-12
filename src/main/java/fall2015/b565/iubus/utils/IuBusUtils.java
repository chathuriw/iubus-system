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

package fall2015.b565.iubus.utils;


import java.io.File;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class IUBusUtils {
    private static Properties properties = new Properties();

    public static String getJDBCUrl(){
        loadProperties();
        return properties.getProperty(Constants.JDBC_URL);
    }

    public static String getJDBCUser () {
        loadProperties();
        return properties.getProperty(Constants.JDBC_USER);
    }

    public static String getJDBCDriver() {
        loadProperties();
        return properties.getProperty(Constants.JDBC_DRIVER);
    }

    public static String getJDBCPWD (){
        loadProperties();
        return properties.getProperty(Constants.JDBC_PWD);
    }

    public static String getResultFolder (){
        loadProperties();
        String baseFolder = properties.getProperty(Constants.RESULT_FOLDER);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        String date = dateFormat.format(cal.getTime());
        baseFolder = baseFolder + File.separator + date + File.separator;
        return baseFolder;
    }

    private static void loadProperties() {
        URL url = getPropertyFileURL();
        try {
            properties.load(url.openStream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static URL getPropertyFileURL() {
        return IUBusUtils.class.getClassLoader().getResource(Constants.IUBUS_PROPERTIES);
    }

    public static String generateAQueryDwellTime(String tableName){
        int[] stopIds = { 39, 38, 37, 41,  1,  4,  6,  8, 10,11, 12, 13, 14, 36, 30, 34, 35, 67};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "avg(e." + stopIds[i] + "- e.to" + stopIds[i] + "),";
        }
        query += "avg(e." + stopIds[stopIds.length-1] + "- e.to" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e." + stopIds[i] + "< 1800 and e.to" + stopIds[i] + " < 1800 and " ;
        }
        query += "e." + stopIds[stopIds.length-1] + " < 1800 and e.to" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateBQueryDwellTime(String tableName){
        int[] stopIds = { 25, 26, 27, 28, 29, 31, 33, 4, 6, 8, 10, 87, 88, 16, 75, 20, 21, 22, 23, 24};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "avg(e.to" + stopIds[i] + "- e.from" + stopIds[i] + "),";
        }
        query += "avg(e.to" + stopIds[stopIds.length-1] + "- e.from" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e.to" + stopIds[i] + "< 1800 and e.from" + stopIds[i] + " < 1800 and " ;
        }
        query += "e.to" + stopIds[stopIds.length-1] + " < 1800 and e.from" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateEQueryDwellTime(String tableName){
        int[] stopIds = {48, 60, 50, 51, 52, 45, 46,  6,  8, 10, 11,12, 13, 14,  1, 55, 57, 58, 59, 107, 61, 62};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "avg(e.to" + stopIds[i] + "- e.from" + stopIds[i] + "),";
        }
        query += "avg(e.to" + stopIds[stopIds.length-1] + "- e.from" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e.to" + stopIds[i] + "< 1800 and e.from" + stopIds[i] + " < 1800 and " ;
        }
        query += "e.to" + stopIds[stopIds.length-1] + " < 1800 and e.from" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateXQueryDwellTime(String tableName){
        int[] stopIds = {118, 64, 78, 76};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "avg(e.to" + stopIds[i] + "- e.from" + stopIds[i] + "),";
        }
        query += "avg(e.to" + stopIds[stopIds.length-1] + "- e.from" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e.to" + stopIds[i] + "< 1800 and e.from" + stopIds[i] + " < 1800 and " ;
        }
        query += "e.to" + stopIds[stopIds.length-1] + " < 1800 and e.from" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateAQueryBetweenStops(String tableName){
        int[] stopIds = { 39, 38, 37, 41,  1,  4,  6,  8, 10,11, 12, 13, 14, 36, 30, 34, 35, 67};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -2 ; i++){
            query += "avg(e." + stopIds[i] + "- e.to" + stopIds[i+1] + "),";
        }
        query += "avg(e." + stopIds[stopIds.length-2] + "- e.to" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length-1 ; i++){
            query += "e." + stopIds[i] + "< 1800 and e.to" + stopIds[i] + " < 1800 and " ;
        }
        query += "e." + stopIds[stopIds.length-1] + " < 1800 and e.to" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateBQueryBetweenStops(String tableName){
        int[] stopIds = { 25, 26, 27, 28, 29, 31, 33, 4, 6, 8, 10, 87, 88, 16, 75, 20, 21, 22, 23, 24};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -2 ; i++){
            query += "avg(e.to" + stopIds[i] + "- e.from" + stopIds[i+1] + "),";
        }
        query += "avg(e.to" + stopIds[stopIds.length-2] + "- e.from" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e.to" + stopIds[i] + "< 1800 and e.from" + stopIds[i] + " < 1800 and " ;
        }
        query += "e.to" + stopIds[stopIds.length-1] + " < 1800 and e.from" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateEQueryBetweenStops(String tableName){
        int[] stopIds = {48, 60, 50, 51, 52, 45, 46,  6,  8, 10, 11,12, 13, 14,  1, 55, 57, 58, 59, 107, 61, 62};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -2 ; i++){
            query += "avg(e.to" + stopIds[i] + "- e.from" + stopIds[i+1] + "),";
        }
        query += "avg(e.to" + stopIds[stopIds.length-2] + "- e.from" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e.to" + stopIds[i] + "< 1800 and e.from" + stopIds[i] + " < 1800 and " ;
        }
        query += "e.to" + stopIds[stopIds.length-1] + " < 1800 and e.from" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }

    public static String generateXQueryBetweenStops(String tableName){
        int[] stopIds = {118, 64, 78, 76};
        String query = "SELECT ";
        for (int i = 0; i < stopIds.length -2 ; i++){
            query += "avg(e.to" + stopIds[i] + "- e.from" + stopIds[i] + "),";
        }
        query += "avg(e.to" + stopIds[stopIds.length-2] + "- e.from" + stopIds[stopIds.length-1] + ") FROM " + tableName + " e where " ;

        for (int i = 0; i < stopIds.length -1 ; i++){
            query += "e.to" + stopIds[i] + "< 1800 and e.from" + stopIds[i] + " < 1800 and " ;
        }
        query += "e.to" + stopIds[stopIds.length-1] + " < 1800 and e.from" + stopIds[stopIds.length-1] + " < 1800";
        return query;
    }


}
