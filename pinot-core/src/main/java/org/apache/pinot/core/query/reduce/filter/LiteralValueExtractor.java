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
package org.apache.pinot.core.query.reduce.filter;

import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Value extractor for a literal.
 */
public class LiteralValueExtractor implements ValueExtractor {
  private final LiteralContext _literal;

  public LiteralValueExtractor(LiteralContext literal) {
    _literal = literal;
  }

  @Override
  public String getColumnName() {
    return _literal.toString();
  }

  @Override
  public ColumnDataType getColumnDataType() {
    ColumnDataType columnDataType = ColumnDataType.fromDataType(_literal.getType(), _literal.isSingleValue());
    // Handle unrecognized result class with STRING
    return columnDataType == ColumnDataType.UNKNOWN ? ColumnDataType.STRING : columnDataType;
  }

  @Override
  public Object extract(Object[] row) {
    return _literal.getValue();
  }
}
