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
import java.util.HashMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.WindowAggregationStorage;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.SumCount;
import ch.epfl.data.squall.types.SumCountType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.OperatorVisitor;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class ApproximateSumSamplingOperator extends OneToOneOperator implements AggregateOperator<SumCount> {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ApproximateSumSamplingOperator.class);

    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;
    private static final int GB_PROJECTION = 1;

    private DistinctOperator _distinct;
    private int _groupByType = GB_UNSET;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private ProjectOperator _groupByProjection;
    private int _numTuplesProcessed = 0;

    private final SumCountType _wrapper = new SumCountType();
    private final ValueExpression _ve;
    private BasicStore<SumCount> _storage;

    private final Map _map;
	private final int _popSize;
	private Map<String, Object> _mapRunningCounts = new HashMap();
	private Map<String, Object> _mapRunningSums = new HashMap();
	private Map<String, Object> _mapRunningSqSums = new HashMap();
	private Map<String, Object> _mapRunningStdDevs = new HashMap();
	private Map<String, Object> _mapRunningMeans = new HashMap();
	
    private boolean isWindowSemantics;
    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    public ApproximateSumSamplingOperator(ValueExpression ve, Map map, int popSize) {
	_ve = ve;
	_map = map;
	_popSize = popSize;
	_storage = new AggregationStorage<SumCount>(this, _wrapper, _map, true);
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

    @Override
    public List<String> getContent() {
	throw new UnsupportedOperationException(
		"getContent for ApproximateSumSamplingOperator is not supported yet.");
    }

    @Override
    public DistinctOperator getDistinct() {
	return _distinct;
    }

    @Override
    public List<ValueExpression> getExpressions() {
	final List<ValueExpression> result = new ArrayList<ValueExpression>();
	result.add(_ve);
	return result;
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
	
	public double tStatLookup(double confidenceTail, long dof) {
		return 1.96;
	}

    @Override
    public String printContent() {
		// String storageContent = _storage.getContent();
		// System.out.println("StorageContent: ");
		// System.out.println(storageContent);
		
		double currentSum = 0;
		long currentCount = 0;
		double currentStdDev = 0;
		double pointEstimateAvg = 0;
		double pSample = 0, avgLower = 0, avgUpper = 0;
		double stdError = 0;
		String resultString = "Number of Tuples Processed: " + _numTuplesProcessed + "\n";

		// double tStat = 1.96;
		double confidence = 0.95;
		double tStat = tStatLookup(1-confidence, currentCount-1);
				
		for (String currentKey : _mapRunningSums.keySet()) {

			currentSum = (double) _mapRunningSums.get(currentKey);
			currentCount = (long) _mapRunningCounts.get(currentKey);
			currentStdDev = (double) _mapRunningStdDevs.get(currentKey);
			
			pointEstimateAvg = 1.0*currentSum/currentCount;
			System.out.print("Key: " + currentKey + " | Sum: " + currentSum);
			System.out.println(" | Count: " + currentCount + " | pointEstimateAvg: " + pointEstimateAvg);
			
			pSample = 1.0*currentCount/_numTuplesProcessed;
			resultString += currentKey;
			resultString += " = " + pointEstimateAvg;	
			
			if (_numTuplesProcessed*pSample <= 10.0) {
				resultString += " | Too early for Interval Estimate.\n";
			}
			else {
				double factor = (tStat*currentStdDev)/Math.sqrt(currentCount);
				avgLower = pointEstimateAvg - factor;
				avgUpper = pointEstimateAvg + factor;
				resultString += " [ " + avgLower + " , " + avgUpper + " ] | 95% Confidence Interval Range = " + (avgUpper - avgLower) + "\n";
			}
		}
		return resultString;

	}
	
	public void updateStatistics(String tupleHash, SumCount sumCount) {
		
		double newRunningSum = sumCount.getSum();
		long newRunningCount = sumCount.getCount();
		
		long oldRunningCount = 0;
		double oldRunningSum = 0;
		double oldRunningSqSum = 0;
		double oldRunningMean = 0;
		double oldRunningStdDev = 0;
		
		if (newRunningCount > 1) {
			oldRunningCount = (long) _mapRunningCounts.get(tupleHash);
			oldRunningSum = (double) _mapRunningSums.get(tupleHash);
			oldRunningSqSum = (double) _mapRunningSqSums.get(tupleHash);
			oldRunningMean = (double) _mapRunningMeans.get(tupleHash);
			oldRunningStdDev = (double) _mapRunningStdDevs.get(tupleHash);
		}
		
		double newValue = newRunningSum - oldRunningSum;
		double newRunningSqSum = oldRunningSqSum + (newValue*newValue);
		double newRunningMean = newRunningSum/newRunningCount;
		double newRunningStdDev = Math.sqrt((newRunningSqSum/newRunningCount) - (newRunningMean*newRunningMean));

		_mapRunningSums.put(tupleHash, newRunningSum);
		_mapRunningCounts.put(tupleHash, newRunningCount);			   
		_mapRunningSqSums.put(tupleHash, newRunningSqSum);
		_mapRunningMeans.put(tupleHash, newRunningMean);
		_mapRunningStdDevs.put(tupleHash, newRunningStdDev);
		
		// System.out.println("Exiting updateStatistics ...");
		
	}
		

    // from Operator
    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;
	if (_distinct != null) {
	    tuple = _distinct.processOne(tuple, lineageTimestamp);
	    if (tuple == null)
		return null;
	}
	String tupleHash;
	if (_groupByType == GB_PROJECTION)
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,  _groupByProjection.getExpressions(), _map);
	else
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,  _map);
	final SumCount sumCount = _storage.update(tuple, tupleHash);
	// System.out.println("TupleHash: " + tupleHash + " | Tuple: " + tuple + " | LTS: " + lineageTimestamp); 
	updateStatistics(tupleHash,sumCount);
	final String strValue = _wrapper.toString(sumCount);

	// propagate further the affected tupleHash-tupleValue pair
	final List<String> affectedTuple = new ArrayList<String>();
	affectedTuple.add(tupleHash);
	affectedTuple.add(strValue);
	
	if (_numTuplesProcessed%5000 ==0) {
		System.out.println("TupleHash: " + tupleHash + " | Tuple: " + tuple + " | LTS: " + lineageTimestamp + " | StrValue: " + strValue); 
		System.out.println("SumCount: " + sumCount); 
		System.out.println("Number of Tuples Processed = " + _numTuplesProcessed);
		System.out.println(printContent());
	}
		
	return affectedTuple;
    }

    // actual operator implementation
    @Override
    public SumCount runAggregateFunction(SumCount value, List<String> tuple) {
	Double sumDelta;
	Long countDelta;

	final Type veType = _ve.getType();
	if (veType instanceof SumCountType) {
	    // when merging results from multiple Components which have SumCount
	    // as the output
	    final SumCount sc = (SumCount) _ve.eval(tuple);
	    sumDelta = sc.getSum();
	    countDelta = sc.getCount();
	} else {
	    final NumericType nc = (NumericType) veType;
	    sumDelta = nc.toDouble(_ve.eval(tuple));
	    countDelta = 1L;
	}

	final Double sumNew = sumDelta + value.getSum();
	final Long countNew = countDelta + value.getCount();

	return new SumCount(sumNew, countNew);
    }

    @Override
    public SumCount runAggregateFunction(SumCount value1, SumCount value2) {
		final Double sumNew = value1.getSum() + value2.getSum();
		final Long countNew = value1.getCount() + value2.getCount();
		return new SumCount(sumNew, countNew);
    }

    @Override
    public ApproximateSumSamplingOperator setDistinct(DistinctOperator distinct) {
	_distinct = distinct;
	return this;
    }

    @Override
    public ApproximateSumSamplingOperator setGroupByColumns(int... hashIndexes) {
	return setGroupByColumns(Arrays
		.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public ApproximateSumSamplingOperator setGroupByColumns(List<Integer> groupByColumns) {
	if (!alreadySetOther(GB_COLUMNS)) {
	    _groupByType = GB_COLUMNS;
	    _groupByColumns = groupByColumns;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public ApproximateSumSamplingOperator setGroupByProjection(
	    ProjectOperator groupByProjection) {
	if (!alreadySetOther(GB_PROJECTION)) {
	    _groupByType = GB_PROJECTION;
	    _groupByProjection = groupByProjection;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggregation already has groupBy set!");
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("ApproximateSumSamplingOperator with VE: ");
	sb.append(_ve.toString());
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
    public AggregateOperator<SumCount> SetWindowSemantics(
	    int windowRangeInSeconds, int windowSlideInSeconds) {
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
    public AggregateOperator<SumCount> SetWindowSemantics(
	    int windowRangeInSeconds) {
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


