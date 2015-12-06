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

public class ApproximateCountOperator extends OneToOneOperator implements AggregateOperator<Long> {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ApproximateCountOperator.class);

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

    private int _field = -1;

    public ApproximateCountOperator(int field, Map map) {
	_field = field;
	_map = map;
	_storage = new AggregationStorage<Long>(this, _wrapper, _map, true);
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
	System.out.println("ZKM: getContent()");
	final String str = _storage.getContent();
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
	System.out.println("ZKM: getStorage()");
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
	System.out.println("ZKM: printContent()");
	return _storage.getContent();
    }

    // from Operator
    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;
	System.out.println("ZKM: ApproxCount.processOne " + _numTuplesProcessed + " " + tuple);

	// extract the field and add it to the sketch.

	// I need list item _field from tuple.
	String tmp_bs = tuple.get(_field);
	int tmp_int = tmp_bs.hashCode();

	AddToSketch(tmp_int);

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

	final Long value = GetSketchMin( tmp_int );
	final String strValue = _wrapper.toString(value);

	// propagate further the affected tupleHash-tupleValue pair
	final List<String> affectedTuple = new ArrayList<String>();
	affectedTuple.add(tupleHash);
	affectedTuple.add(strValue);

	System.out.println("ZKM: AppoxCount result " + affectedTuple);
	return affectedTuple;
    }

    // actual operator implementation
    @Override
    public Long runAggregateFunction(Long value, List<String> tuple) {
	System.out.println("ZKM: ApproxCount AGG1 " + value + " " + tuple);
	return value + 1;
    }

    @Override
    public Long runAggregateFunction(Long value1, Long value2) {
	System.out.println("ZKM: ApproxCount AGG2 " + value1 + " " + value2);
	return value1 + value2;
    }

    @Override
    public ApproximateCountOperator setDistinct(DistinctOperator distinct) {
	_distinct = distinct;
	return this;
    }

    @Override
    public ApproximateCountOperator setGroupByColumns(int... hashIndexes) {
	return setGroupByColumns(Arrays
		.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public ApproximateCountOperator setGroupByColumns(List<Integer> groupByColumns) {
	if (!alreadySetOther(GB_COLUMNS)) {
	    _groupByType = GB_COLUMNS;
	    _groupByColumns = groupByColumns;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public ApproximateCountOperator setGroupByProjection(
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
	sb.append("AggregateCountOperator ");
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



    public static int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                (l + " cannot be cast to int without changing its value.");
        }
		int zz = (int) l;
		if(zz > 0) {
			return zz;
		} else {
			return -zz;
		}
    }

    ////////////////////////////////
    // Count-Min.
    ////////////////////////////////
    //
    // These should of course all be initialized in the constructor, not
    // hard-coded.  Prime numbers should be generated based on a seed.
    // Also, the dimension needs to be variable (_y) and not "3" as it is
    // currently implemented.

    public int _x = 997;
    public int _y = 3;

    public long[] _P1 = {5915587277L, 1500450271L, 3267000013L};
    public long[] _P2 = {5754853343L, 4093082899L, 9576890767L};
    public long[] _P3 = {3628273133L, 2860486313L, 5463458053L};
    public long[] _P4 = {13L,         17L,         19L        };

    // Actual sketch grid
    public long[][] _sketch = new long[_x][_y];

    public boolean AddToSketch(long k) {

        System.out.println("ADD SKETCH FOR " + k);

        for (int y = 0; y < _y; y++) {
            int x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);

            System.out.println("C[" + x + "][" + y + "] ... ");
            System.out.println("C[" + x + "][" + y + "] was " + _sketch[x][y]);
            _sketch[x][y]++;
            System.out.println("C[" + x + "][" + y + "] now " + _sketch[x][y]);
        }
		return true;
    }


    public long GetSketchMin(long k) {

        System.out.println("GET SKETCH FOR " + k);

        //$y = 0;
        int y = 0;
        //$x = (($k * $P1[$y] + $P2[$y]) * $P3[$y] + $P4[$y]) % $WIDTH;
        int x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);
        //$v = $C[$x][$y];
        long v = _sketch[x][y];

        //print "C[$x][$y] == $v\n";
        System.out.println("C[" + x + "][" + y + "] == " + _sketch[x][y]);

        for (y = 1; y < 3; y++) {
            //$x = (($k * $P1[$y] + $P2[$y]) * $P3[$y] + $P4[$y]) % $WIDTH;
            x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);
            System.out.println("C[" + x + "][" + y + "] == " + _sketch[x][y]);
            if (_sketch[x][y] < v) {
                v = _sketch[x][y];
            }
        }
        return v;
    }

}

