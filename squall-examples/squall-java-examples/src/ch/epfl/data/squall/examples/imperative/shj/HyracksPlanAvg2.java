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

package ch.epfl.data.squall.examples.imperative.shj;

import java.util.Map;
import java.util.Arrays;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateAvgOperator;
import ch.epfl.data.squall.operators.ApproximateAvgSamplingOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.NumericType;

public class HyracksPlanAvg2 extends QueryPlan {
	
	private static final NumericType<Double> _doubleConv = new DoubleType();

    public HyracksPlanAvg2(String dataPath, String extension, Map conf) {
        super(dataPath, extension, conf);
    }

    @Override
    public Component createQueryPlan(String dataPath, String extension, Map conf) {

		final AggregateOperator agg = new ApproximateAvgSamplingOperator(new ColumnReference(_doubleConv, 0), conf, 1500000)
				     						.setGroupByColumns(Arrays.asList(1));
		
		// final AggregateOperator agg = new AggregateAvgOperator(new ColumnReference(_doubleConv, 0), conf)
		//		     						.setGroupByColumns(Arrays.asList(1));
		
		Component orders = new DataSourceComponent("orders", conf)
                			.add(new ProjectOperator(3,5))
							.add(agg);

        return orders;
		
        // -------------------------------------------------------------------------------------
    }
}
