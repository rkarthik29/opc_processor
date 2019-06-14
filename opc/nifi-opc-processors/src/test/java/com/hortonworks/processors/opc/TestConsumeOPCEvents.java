/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.processors.opc;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestConsumeOPCEvents {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConsumeOPCEvents.class);
    }

    @Test
    public void testProcessor() {
        // testRunner.setRunSchedule(60000);
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(ConsumeOPCEvents.OPC_SERVER_URL, "opc.tcp://HW13386.local:53530/OPCUA/SimulationServer");
        testRunner.setProperty(ConsumeOPCEvents.NAME_SPACE_INDEX, "5");
        testRunner.setProperty(ConsumeOPCEvents.SUB_DUR, "5");
        testRunner.run(1);
        assert (testRunner.getFlowFilesForRelationship(ConsumeOPCEvents.SUCCESS).size() == 1);

    }

}
