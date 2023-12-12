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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


public class AggregationSpec {
  public static final AggregationSpec DEFAULT =
      new AggregationSpec(ChunkCompressionType.PASS_THROUGH, Collections.emptyList());

  private final ChunkCompressionType _compressionType;
  private final List<String> _virtualAggregationFunctions;

  public AggregationSpec(ChunkCompressionType compressionType, List<String> virtualAggregationFunctions) {
    _compressionType = compressionType;
    _virtualAggregationFunctions = virtualAggregationFunctions;
  }

  public ChunkCompressionType getCompressionType() {
    return _compressionType;
  }

  public List<String> getVirtualAggregationFunctions() {
    return _virtualAggregationFunctions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AggregationSpec)) {
      return false;
    }
    AggregationSpec that = (AggregationSpec) o;
    return _compressionType == that._compressionType && _virtualAggregationFunctions.equals(
        that._virtualAggregationFunctions);
  }

  @Override
  public int hashCode() {
    int result = _compressionType.hashCode();
    result = 31 * result + _virtualAggregationFunctions.hashCode();
    return result;
  }
}
