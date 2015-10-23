/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015 Pentaho Corporation.  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.olap.Result;
import mondrian.rolap.BatchTestCase;
import mondrian.test.TestContext;

/**
 * Tests for UnionFunDef
 *
 * @author tsantos
 */
public class NonEmptyFunDefTest  extends BatchTestCase {

    public void testNonEmptyFun() throws Exception {
        final String query =
            "select nonempty(time.month.members) on 0 from sales";

        final String query2 =
            "select nonempty(time.month.members, measures.[unit sales]) on 0 from sales";

        final String expected =
            "select non empty time.month.members on 0 from sales";

        TestContext context = getTestContext();

        Result resultFunDef = context.executeQuery(query);
        Result resultFunDef2 = context.executeQuery(query2);
        Result resultExpected = context.executeQuery(expected);

        assertEquals(
            TestContext.toString(resultFunDef),
            TestContext.toString(resultExpected));

        assertEquals(
            TestContext.toString(resultFunDef2),
            TestContext.toString(resultExpected));
    }
}
