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
package org.apache.beam.sdks.java.api.component;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Mark a method as returning a restriction.
 *
 * <p>Supported signatures are:
 *
 * <ul>
 *   <li><code>MyRestriction initialRestriction()</code>: initialize the restriction state
 *   <li>
 *       <pre>
 *       <code>void split(@Element E element, Consumer&lt;MyRestriction&gt; restrictionConsumer)</code>
 *       </pre>
 *       : split a restriction in N (&gt;= 0) restriction(s), means the consumer can be called N
 *       times. Parameter matching is done by type for the consumer and thanks annotations for the
 *       other parameters.
 * </ul>
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface DefineRestriction {}
