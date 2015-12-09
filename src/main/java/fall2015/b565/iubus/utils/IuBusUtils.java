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
}
