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
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ApproximateSumSamplingOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.NumericType;

public class HyracksPlanSum extends QueryPlan {
	
	private static final NumericType<Double> _doubleConv = new DoubleType();

    public HyracksPlanSum(String dataPath, String extension, Map conf) {
        super(dataPath, extension, conf);
    }

    @Override
    public Component createQueryPlan(String dataPath, String extension, Map conf) {
        // -------------------------------------------------------------------------------------
        Component customer = new DataSourceComponent("customer", conf)
				.add(new ProjectOperator(0, 5, 6));	

        // -------------------------------------------------------------------------------------
        Component orders = new DataSourceComponent("orders", conf)
                .add(new ProjectOperator(1));

        // -------------------------------------------------------------------------------------
        // final AggregateOperator agg = new AggregateSumOperator(
		//									new ColumnReference(_doubleConv, 1), conf)
		//									.setGroupByColumns(Arrays.asList(2));
		
		final AggregateOperator agg = new ApproximateSumSamplingOperator(
											new ColumnReference(_doubleConv, 1), conf, 15000)
											.setGroupByColumns(Arrays.asList(2));
		
		Component custOrders = new EquiJoinComponent(customer, 0, orders, 0)
									.add(new SampleOperator(0.1))
									.add(agg);
		
		return custOrders;
        // -------------------------------------------------------------------------------------
    }
}
