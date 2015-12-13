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

import java.sql.Date;
import java.sql.Time;
import java.util.Map;

public class ActualSchedule {
    private int route;
    private Date date;
    private int totalRidership;
    private Map<Time, Time[]> allSchedules;

    public ActualSchedule(int route, Date date) {
        this.route = route;
        this.date = date;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getRoute() {
        return route;
    }

    public void setRoute(int route) {
        this.route = route;
    }

    public Map<Time, Time[]> getAllSchedules() {
        return allSchedules;
    }

    public void setAllSchedules(Map<Time, Time[]> allSchedules) {
        this.allSchedules = allSchedules;
    }
}
