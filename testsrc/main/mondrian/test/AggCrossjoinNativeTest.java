/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2016 Pentaho Corporation
// All Rights Reserved.
*/
package mondrian.test;


public class AggCrossjoinNativeTest extends FoodMartTestCase {
    public void testCompoundAggCalcMemberInSlicer() {
        String query = "WITH member store.agg as "
            + "'Aggregate(CrossJoin(Store.[Store Name].members, Gender.F))' "
            + "SELECT filter(customers.[name].members, measures.[unit sales] > 100) on 0 "
            + "FROM sales where store.agg";

        verifySameNativeAndNot(
            query,
            "Compound aggregated member should return same results with native filter on/off",
            getTestContext());
    }
}
