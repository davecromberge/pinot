/**
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
package org.apache.pinot.segment.local.startree.v2;

import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;


public class StarTreeBuilderUtilsTest {

  @Test
  public void testResolveToAggregatedType() {
    assertEquals(AggregationFunctionColumnPair.fromColumnName("distinctCountThetaSketch__dimX"),
        StarTreeBuilderUtils.resolveToAggregatedType(
            AggregationFunctionColumnPair.fromColumnName("distinctCountRawThetaSketch__dimX")));
    assertEquals(AggregationFunctionColumnPair.fromColumnName("count__*"),
        StarTreeBuilderUtils.resolveToAggregatedType(AggregationFunctionColumnPair.fromColumnName("count__*")));
    assertEquals(AggregationFunctionColumnPair.fromColumnName("sum__dimY"),
        StarTreeBuilderUtils.resolveToAggregatedType(AggregationFunctionColumnPair.fromColumnName("sum__dimY")));
  }

}
