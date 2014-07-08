/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.io.TempFileCreator;
import com.microsoft.reef.io.WorkingDirectoryTempFileCreator;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;
import com.microsoft.reef.runtime.common.parameters.JVMHeapSlack;
import com.microsoft.reef.runtime.yarn.driver.*;
import com.microsoft.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import com.microsoft.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod;
import com.microsoft.reef.runtime.yarn.util.YarnConfigurationConstructor;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.time.Clock;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * ConfigurationModule to create a Driver configuration.
 */
@Private
@ClientSide
public final class HDInsightDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * @see com.microsoft.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory
   */
  public static final RequiredParameter<String> JOB_SUBMISSION_DIRECTORY = new RequiredParameter<>();
  /**
   * @see com.microsoft.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod.class
   */
  public static final OptionalParameter<Integer> YARN_HEARTBEAT_INTERVAL = new OptionalParameter<>();

  /**
   * @see AbstractDriverRuntimeConfiguration.JobIdentifier.class
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see AbstractDriverRuntimeConfiguration.EvaluatorTimeout
   */
  public static final OptionalParameter<Long> EVALUATOR_TIMEOUT = new OptionalParameter<>();

  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new HDInsightDriverConfiguration()

      // Bind the YARN runtime for the resource manager.
      .bindImplementation(ResourceLaunchHandler.class, YARNResourceLaunchHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, YARNResourceReleaseHandler.class)
      .bindImplementation(ResourceRequestHandler.class, YarnResourceRequestHandler.class)
      .bindConstructor(YarnConfiguration.class, YarnConfigurationConstructor.class)
      .bindSetEntry(Clock.RuntimeStartHandler.class, YARNRuntimeStartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, YARNRuntimeStopHandler.class)
      .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)

          // Bind the YARN Configuration parameters
      .bindNamedParameter(JobSubmissionDirectory.class, JOB_SUBMISSION_DIRECTORY)
      .bindNamedParameter(YarnHeartbeatPeriod.class, YARN_HEARTBEAT_INTERVAL)

          // Bind the fields bound in AbstractDriverRuntimeConfiguration
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.EvaluatorTimeout.class, EVALUATOR_TIMEOUT)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .build();
}
