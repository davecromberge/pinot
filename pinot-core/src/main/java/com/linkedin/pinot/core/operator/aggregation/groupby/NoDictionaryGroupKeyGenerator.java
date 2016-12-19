/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.clearspring.analytics.util.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionColumnBlock;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Implementation of {@link GroupKeyGenerator} interface using actual string based
 * group keys, instead of dictionary ids. This implementation is used for group-by key
 * generation when one or more of the group-by columns do not have dictionary.
 *
 * TODO:
 * 1. Add support for multi-valued group-by columns.
 * 2. Add support for trimming group-by results.
 */
public class NoDictionaryGroupKeyGenerator implements GroupKeyGenerator {

  private String[] _groupByColumns;
  Map<String, Integer> _groupKeyMap;
  private int _numGroupKeys = 0;

  /**
   * Constructor for the class.
   *
   * @param groupByColumns Columns for which to generate group-by keys
   */
  public NoDictionaryGroupKeyGenerator(String[] groupByColumns) {
    _groupByColumns = groupByColumns;
    _groupKeyMap = new HashMap<>();
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    // Since there's no dictionary, we cannot find the cardinality
    return Integer.MAX_VALUE;
  }

  @Override
  public void generateKeysForBlock(ProjectionBlock projectionBlock, int[] docIdToGroupKey) {
    int numGroupByColumns = _groupByColumns.length;
    int numDocs = projectionBlock.getNumDocs();

    Object[] values = new Object[numGroupByColumns];
    FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[numGroupByColumns];

    for (int i = 0; i < numGroupByColumns; i++) {
      ProjectionColumnBlock dataBlock = (ProjectionColumnBlock) projectionBlock.getDataBlock(_groupByColumns[i]);
      BlockValSet blockValSet = dataBlock.getBlockValueSet();

      // This method is only for single-valued columns, there's a separate method for multi-valued columns.
      Preconditions.checkState(dataBlock.getMetadata().isSingleValue(),
          "Multi-valued column specified in single valued group key generator: " + _groupByColumns[i]);

      dataTypes[i] = blockValSet.getValueType();
      values[i] = blockValSet.getSingleValues();
    }

    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < numDocs; i++) {
      stringBuilder.setLength(0);

      for (int j = 0; j < numGroupByColumns; j++) {
        double[] doubleValues;

        // BlockValSet.getSingleValues() always returns double currently, as all aggregation functions assume
        // data type to be double.

        switch (dataTypes[j]) {
          case INT:
            doubleValues = (double[]) values[j];
            stringBuilder.append((int) doubleValues[i]);
            break;

          case LONG:
            doubleValues = (double[]) values[j];
            stringBuilder.append((long) doubleValues[i]);
            break;

          case FLOAT:
            doubleValues = (double[]) values[j];
            stringBuilder.append((float) doubleValues[i]);
            break;

          case DOUBLE:
            doubleValues = (double[]) values[j];
            stringBuilder.append(doubleValues[i]);
            break;

          default:
            Object[] objects = (Object[]) values[j];
            stringBuilder.append(objects[i].toString());
            break;
        }

        if (j < (numGroupByColumns - 1)) {
          stringBuilder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter);
        }
      }

      docIdToGroupKey[i] = getGroupIdForKey(stringBuilder.toString());
    }
  }

  @Override
  public void generateKeysForBlock(ProjectionBlock projectionBlock, int[][] docIdToGroupKeys) {
    // TODO: Support generating keys for multi-valued columns.
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    return new GroupKeyIterator(_groupKeyMap);
  }

  @Override
  public void purgeKeys(int[] keysToPurge) {
    // TODO: Implement purging.
    throw new UnsupportedOperationException("Purging keys not yet supported in GroupKeyGenerator without dictionary.");
  }

  /**
   * Helper method to get or create group-id for a group key.
   *
   * @param key Group key for which to generate group id
   * @return Group id
   */
  private int getGroupIdForKey(String key) {
    int groupId;

    if (!_groupKeyMap.containsKey(key)) {
      _groupKeyMap.put(key, _numGroupKeys);
      groupId = _numGroupKeys++;
    } else {
      groupId = _groupKeyMap.get(key);
    }

    return groupId;
  }

  /**
   * Iterator for {Group-Key, Group-id) pair.
   */
  class GroupKeyIterator implements Iterator<GroupKey> {
    Iterator<Map.Entry<String, Integer>> _iterator;
    GroupKey _groupKey;

    public GroupKeyIterator(Map<String, Integer> map) {
      _iterator = map.entrySet().iterator();
      _groupKey = new GroupKey(INVALID_ID, null);
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Map.Entry<String, Integer> entry = _iterator.next();
      _groupKey.setFirst(entry.getValue());
      _groupKey.setSecond(entry.getKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
