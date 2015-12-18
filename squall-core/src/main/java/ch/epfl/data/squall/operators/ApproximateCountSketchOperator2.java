/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.WindowAggregationStorage;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.OperatorVisitor;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

import ch.epfl.data.squall.utilities.SketchCountMin;

public class ApproximateCountSketchOperator2 extends OneToOneOperator implements AggregateOperator<Long> {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ApproximateCountSketchOperator2.class);

    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;
    private static final int GB_PROJECTION = 1;

    private DistinctOperator _distinct;
    private int _groupByType = GB_UNSET;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private ProjectOperator _groupByProjection;
    private int _numTuplesProcessed = 0;

    private final NumericType<Long> _wrapper = new LongType();
    private BasicStore<Long> _storage;

    private final Map _map;

    private boolean isWindowSemantics;
    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    private int _field;
    private HashSet<String> _unique_keys;

    private SketchCountMin _scm;

    public ApproximateCountSketchOperator2(int field, int x, int y, Map map) {
	_field = field;
	_unique_keys = new HashSet<String>();
	_map = map;
	_storage = new AggregationStorage<Long>(this, _wrapper, _map, true);
	_scm = new SketchCountMin(x, y);
	System.out.println("ZKM: _map is " + map);
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    private boolean alreadySetOther(int GB_COLUMNS) {
	return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
    }

    @Override
    public void clearStorage() {
	_storage.reset();
    }

    // for this method it is essential that HASH_DELIMITER, which is used in
    // tupleToString method,
    // is the same as DIP_GLOBAL_ADD_DELIMITER
    @Override
    public List<String> getContent() {
	final String str = _storage.getContent();
	System.out.println("ZKM: getContent str = " + str);
	return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
    }

    @Override
    public DistinctOperator getDistinct() {
	return _distinct;
    }

    @Override
    public List<ValueExpression> getExpressions() {
	return new ArrayList<ValueExpression>();
    }

    @Override
    public List<Integer> getGroupByColumns() {
	return _groupByColumns;
    }

    @Override
    public ProjectOperator getGroupByProjection() {
	return _groupByProjection;
    }

    private String getGroupByStr() {
	final StringBuilder sb = new StringBuilder();
	sb.append("(");
	for (int i = 0; i < _groupByColumns.size(); i++) {
	    sb.append(_groupByColumns.get(i));
	    if (i == _groupByColumns.size() - 1)
		sb.append(")");
	    else
		sb.append(", ");
	}
	return sb.toString();
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    @Override
    public BasicStore getStorage() {


	Iterator<String> tuples = _unique_keys.iterator();
	String tuple;
	List<String> faketuple;

	while (tuples.hasNext()) {
	   tuple = tuples.next();
//	   System.out.println("ZKM: _unique_keys tuple: " + tuple);
	   faketuple = Arrays.asList(tuple);
	   _storage.update(faketuple, tuple);
	}

	return _storage;
    }

    @Override
    public Type getType() {
	return _wrapper;
    }

    @Override
    public boolean hasGroupBy() {
	return _groupByType != GB_UNSET;
    }

    @Override
    public boolean isBlocking() {
	return true;
    }

    @Override
    public String printContent() {
	return _storage.getContent();
    }

    // from Operator
    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
//	System.out.println("ZKM: processOne(" + tuple + ")");
	_numTuplesProcessed++;
	if (_distinct != null) {
	    tuple = _distinct.processOne(tuple, lineageTimestamp);
	    if (tuple == null)
		return null;
	}
	String tupleHash;
	if (_groupByType == GB_PROJECTION)
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
		    _groupByProjection.getExpressions(), _map);
	else
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
		    _map);

//	final Long value = _storage.update(tuple, tupleHash);
//	final String strValue = _wrapper.toString(value);
	final String strValue = "1";

	// propagate further the affected tupleHash-tupleValue pair
	final List<String> affectedTuple = new ArrayList<String>();
	affectedTuple.add(tupleHash);
	affectedTuple.add(strValue);

	long k = (new Long(tupleHash)).longValue();
	long v = _scm.UpdateSketch(k, 1);
//	System.out.println("ZKM: " + k + " has count " + v + ", adding \"" + tupleHash + "\" to _unique_keys");
	_unique_keys.add(tupleHash);

//	System.out.println("ZKM: affectedTuple == " + affectedTuple);
	return affectedTuple;
    }

    // actual operator implementation
    @Override
    public Long runAggregateFunction(Long value, List<String> tuple) {
	//System.out.println("ZKM: **NOOP** runAggregateFunction(" + value + " " + tuple + ")");

        // I need list item _field from tuple.
        String tmp_bs = tuple.get(_field);
//	long hash = tmp_bs.hashCode();
	long hash = (new Long(tmp_bs).longValue());
	final Long v = _scm.UpdateSketch(hash, 0);
//	System.out.println("ZKM: runAggregateFunction(" + hash  + ") == " + v);
	return v;

//	return 0L;
    }

    @Override
    public Long runAggregateFunction(Long value1, Long value2) {
//	System.out.println("ZKM: runAggregateFunction(" + value1 + " " + value2 + ")");
	return value1 + value2;
    }

    @Override
    public ApproximateCountSketchOperator2 setDistinct(DistinctOperator distinct) {
	_distinct = distinct;
	return this;
    }

    @Override
    public ApproximateCountSketchOperator2 setGroupByColumns(int... hashIndexes) {
	return setGroupByColumns(Arrays
		.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public ApproximateCountSketchOperator2 setGroupByColumns(List<Integer> groupByColumns) {
	if (!alreadySetOther(GB_COLUMNS)) {
	    _groupByType = GB_COLUMNS;
	    _groupByColumns = groupByColumns;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public ApproximateCountSketchOperator2 setGroupByProjection(
	    ProjectOperator groupByProjection) {
	if (!alreadySetOther(GB_PROJECTION)) {
	    _groupByType = GB_PROJECTION;
	    _groupByProjection = groupByProjection;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("ApproximateCountSketchOperator2 ");
	if (_groupByColumns.isEmpty() && _groupByProjection == null)
	    sb.append("\n  No groupBy!");
	else if (!_groupByColumns.isEmpty())
	    sb.append("\n  GroupByColumns are ").append(getGroupByStr())
		    .append(".");
	else if (_groupByProjection != null)
	    sb.append("\n  GroupByProjection is ")
		    .append(_groupByProjection.toString()).append(".");
	if (_distinct != null)
	    sb.append("\n  It also has distinct ").append(_distinct.toString());
	return sb.toString();
    }

    @Override
    public AggregateOperator<Long> SetWindowSemantics(int windowRangeInSeconds,
	    int windowSlideInSeconds) {
	WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
	isWindowSemantics = true;
	_windowRangeSecs = windowRangeInSeconds;
	_slideRangeSecs = windowSlideInSeconds;
	_storage = new WindowAggregationStorage<>(this, _wrapper, _map, true,
		_windowRangeSecs, _slideRangeSecs);
	if (_groupByColumns != null || _groupByProjection != null)
	    _storage.setSingleEntry(false);
	return this;
    }

    @Override
    public AggregateOperator<Long> SetWindowSemantics(int windowRangeInSeconds) {
	return SetWindowSemantics(windowRangeInSeconds, windowRangeInSeconds);
    }

    @Override
    public int[] getWindowSemanticsInfo() {
	int[] res = new int[2];
	res[0] = _windowRangeSecs;
	res[1] = _slideRangeSecs;
	return res;
    }
}
