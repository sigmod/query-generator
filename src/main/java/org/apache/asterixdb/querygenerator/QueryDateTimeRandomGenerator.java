/*
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
 */
package org.apache.asterixdb.querygenerator;

import datatype.Date;
import generator.RandomDateGenerator;

public class QueryDateTimeRandomGenerator extends RandomDateGenerator {

    private static long THREE_MONTH = 1000L * 3600 * 24 * 30L * 3 + 1;

    public QueryDateTimeRandomGenerator(Date startDate, Date endDate, long seed) {
        super(startDate, endDate, seed);
        this.diff = (eDate - sDate - THREE_MONTH) + 1;
    }

}