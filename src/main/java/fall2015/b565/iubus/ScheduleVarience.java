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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Set;

public class ScheduleVarience {
    private static Logger log = LoggerFactory.getLogger(ScheduleVarience.class);
    public static void main(String[] args) throws Exception{
        try {
            DataReader reader = new DataReader();
            String time1 = "07:15:00";
            String time2 = "07:35:00";
            String scheduleTime = "07:25:00";
            SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
            long ms1 = sdf.parse(time1).getTime();
            long ms2 = sdf.parse(time2).getTime();
            long ms3 = sdf.parse(scheduleTime).getTime();
            Time t1 = new Time(ms1);
            Time t2 = new Time(ms2);
            Time t3 = new Time(ms3);
            Map<Time, Time> timeMap = reader.getTime(t1, t2, t3, 331, 67);
            double variance = getVariance(timeMap.keySet());
            System.out.println("Variance : " + variance);
//            for (Time actualTime : timeMap.keySet()){
//                System.out.println("Actual Time : " + actualTime + " scheduled time : " + timeMap.get(actualTime));
//                actualTime.getTime();
//            }
        } catch (ParseException e) {
            log.error("Error while parsing date", e);
            throw new RuntimeException("Error while parsing date", e);
        }
    }

    private static long getMean(Set<Time> timeSet){
        long accumilatedTime = 0;
        for (Time time : timeSet){
            accumilatedTime += time.getTime();
        }
        long mean = accumilatedTime / timeSet.size();
        System.out.println("Mean : " + new Time(mean));

        return mean;
    }

    private static double getVariance(Set<Time> timeSet) {
        long temp = 0;
        long mean = getMean(timeSet);
        for (Time time : timeSet){
            temp += (mean - time.getTime()) * (mean - time.getTime());
        }
        return temp/timeSet.size();
    }
}
