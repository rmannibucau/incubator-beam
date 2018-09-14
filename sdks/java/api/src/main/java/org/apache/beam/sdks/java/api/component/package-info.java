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

/**
 * Contains the generic API to write atomic "component" (transform element).
 *
 * <p>A component is composed of a set of method marked with annotations:
 *
 * <ul>
 *   <li>
 *       <pre><code>@DefineRestriction</code></pre>
 *       : defines how to track current iteration "state" on the overall dataset and how to split a
 *       subdataset in other subdatasets to parallelize the processing if possible.
 *   <li>
 *       <pre><code>@OnElement</code></pre>
 *       : defines how to process an element.
 *   <li>
 *       <pre><code>@Setup</code></pre>
 *       and
 *       <pre><code>@Teardown</code></pre>
 *       to manage the instance lifecycle.
 *   <li>
 *       <pre><code>@BeforeBundle</code></pre>
 *       and
 *       <pre><code>@AfterBundle</code></pre>
 *       to manage the bundles lifecycle.
 * </ul>
 *
 * Note that a component must be {@link java.io.Serializable} and that there is no guarantee on the
 * number of instances of a component, the runner will create as much as needed instances for to
 * complete its job.
 */
package org.apache.beam.sdks.java.api.component;
