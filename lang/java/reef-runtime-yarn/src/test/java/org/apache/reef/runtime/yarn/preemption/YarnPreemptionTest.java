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
package org.apache.reef.runtime.yarn.preemption;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.*;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tests whether REEF can handle PreemptedEvaluator when a YARN container is preempted.
 */
public class YarnPreemptionTest {

  private static final Logger LOG = Logger.getLogger(YarnPreemptionTest.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 100000; // 100 sec.

  private static final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();

  private static final Configuration testConfigurationB = YarnPreemptionTestConfiguration.CONF
      .set(YarnPreemptionTestConfiguration.JOB_QUEUE, "B")
      .build();

  private static final Tang tang = Tang.Factory.getTang();

  private static final Configuration driverConfigurationB = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(YarnPreemptionTest.class))
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_YarnPreemptionTest_Preemptee")
      .set(DriverConfiguration.ON_DRIVER_STARTED, YarnPreemptionTestPreempteeDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
          YarnPreemptionTestPreempteeDriver.EvaluatorAllocatedHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_PREEMPTED,
          YarnPreemptionTestPreempteeDriver.EvaluatorPreemptedHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_FAILED,
          YarnPreemptionTestPreempteeDriver.EvaluatorFailedHandler.class)
      .build();

  private static final Configuration mergedDriverConfigurationB =
      tang.newConfigurationBuilder(driverConfigurationB, testConfigurationB).build();


  private static final Configuration testConfigurationA = YarnPreemptionTestConfiguration.CONF
      .set(YarnPreemptionTestConfiguration.JOB_QUEUE, "A")
      .build();

  private static final Configuration driverConfigurationA = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(YarnPreemptionTest.class))
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_YarnPreemptionTest_Preemptor")
      .set(DriverConfiguration.ON_DRIVER_STARTED, YarnPreemptionTestPreemptorDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
          YarnPreemptionTestPreemptorDriver.EvaluatorAllocatedHandler.class)
      .build();

  private static final Configuration mergedDriverConfigurationA =
      tang.newConfigurationBuilder(driverConfigurationA, testConfigurationA).build();


  private void runYarnPreemptionTest() throws InjectionException, InterruptedException {

    final Thread preempteeThread = new Thread() {
      public void run() {
        try {
          final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
              .run(mergedDriverConfigurationB, JOB_TIMEOUT);
          Assert.assertTrue("Job B (preemptee) state after execution: " + state, state.isDone());
        } catch (final InjectionException e) {
          LOG.log(Level.SEVERE, "Invalid configuration", e);
        }
      }
    };

    final Thread preemptorThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(6000);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Interrupt occurred", e);
        }

        try {
          final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
              .run(mergedDriverConfigurationA, JOB_TIMEOUT);
          Assert.assertTrue("Job A (preemptor) state after execution: " + state, state.isSuccess());
        } catch (final InjectionException e) {
          LOG.log(Level.SEVERE, "Invalid configuration", e);
        }
      }
    };

    preempteeThread.start();
    preemptorThread.start();

    preempteeThread.join();
    preemptorThread.join();
  }

  @Test
  public void testYarnPreemption() throws InjectionException, InterruptedException {
    Assume.assumeTrue("This test requires a YARN Resource Manager to connect to",
        Boolean.parseBoolean(System.getenv("REEF_TEST_YARN")));
    runYarnPreemptionTest();
  }
}
