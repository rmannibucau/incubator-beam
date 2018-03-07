/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdks.state.hazelcast;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The configuration of the hazelcast "connection".
 */
public interface HazelcastStateOptions extends PipelineOptions {
    @Default.String("auto")
    @Description("The classpath location of the configuration.")
    String getHazelcastStateStoreConfigurationLocation();
    void setHazelcastStateStoreConfigurationLocation(String location);

    @Default.String("apache-beam")
    @Description("The prefix used by the underlying Hazelcast structures.")
    String getHazelcastStateStorePrefix();
    void setHazelcastStateStorePrefix(String prefix);
}
