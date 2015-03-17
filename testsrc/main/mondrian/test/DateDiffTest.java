/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2015 Pentaho and others
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;

/**
 * Tests DateDiff
 *
 * @author tsantos
 * @since January 15, 2015
 */
public class DateDiffTest extends FoodMartTestCase {
    public DateDiffTest() {
        super();
    }

    public DateDiffTest(String name) {
        super(name);
    }

    public void testDateDiffYear() {
        String s = executeExpr2("DateDiff('y', Time.[1997], Time.[1998])");
        assertEquals("1", s);

        s = executeExpr2("DateDiff('YEAR', Time.[1998], Time.[1997])");
        assertEquals("-1", s);

        // invalid member throw error
        assertExprThrows(
            "DateDiff('y', Time.[1998], Time.[2015])",
            "MDX object 'Time.[2015]' not found in cube 'Sales'");

        // ignoring invalid members should succeed
        propSaver.set(
            MondrianProperties.instance().IgnoreInvalidMembersDuringQuery,
            true);
        s = executeExpr2("DateDiff('y', Time.[1998], Time.[2015])");
        assertEquals("17", s);
        propSaver.reset();

        s = executeExpr2(
            "DateDiff('y', "
            + "MockCurrentDateMember([Time].[Time], \"[yyyy]\"), Time.[1998])");
        assertEquals("1", s);

        s = executeExpr2(
            "DateDiff('year',"
            + "Time.[1998], MockCurrentDateMember([Time].[Time], \"[yyyy]\"))");
        assertEquals("-1", s);

        s = executeExpr2("DateDiff('Year', [Time.Weekly].[1997], Time.[1997])");
        assertEquals("0", s);
    }

    public void testDateDiffQuarter() {
        String s = executeExpr2(
            "DateDiff('q', Time.[1997].[Q1], Time.[1998].[Q1].[3])");
        assertEquals("4", s);

        s = executeExpr2(
            "DateDiff('q', Time.[1997].[Q1], [Time.Weekly].[1998].[19])");
        assertEquals("5", s);

        s = executeExpr2(
            "DateDiff('quarter', Time.[1998].[Q1], Time.[1997].[Q1])");
        assertEquals("-4", s);

        s = executeExpr2(
            "DateDiff('q',"
            + "MockCurrentDateMember([Time].[Time], \"[yyyy]\"),"
            + "Time.[1997].[Q4].[12])");
        assertEquals("3", s);
    }

    public void testDateDiffMonth() {
        String s = executeExpr2(
            "DateDiff('m', Time.[1997].[Q1], Time.[1997].[Q2])");
        assertEquals("3", s);

        s = executeExpr2("DateDiff('m', Time.[1997].[Q2], Time.[1997].[Q1])");
        assertEquals("-3", s);

        s = executeExpr2(
            "DateDiff('month', Time.[1997].[Q1].[2], Time.[1997].[Q2].[6])");
        assertEquals("4", s);
    }

    public void testDateDiffDay() {
        String s = executeExpr2(
            "DateDiff('d', Time.[1997].[Q1], Time.[1997].[Q1])");
        assertEquals("0", s);

        s = executeExpr2(
            "DateDiff('day', Time.[1997].[Q1], [Time].[1998].[Q1])");
        assertEquals("365", s);

        s = executeExpr2(
            "DateDiff('day',"
            + "MockCurrentDateMember([Time].[Time], \"[yyyy]\\.[\\Qq]\\.[m]\"),"
            + " [Time].[1998].[Q1])");
        assertEquals("334", s);
    }

    public String executeExpr2(String expression){
        TestContext context = TestContext.instance().create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"MockCurrentDateMember\" "
                + "className=\"mondrian.udf.MockCurrentDateMember\" /> ",
            null);

        return context.executeExprRaw(expression).getFormattedValue();
    }

}

// End FilterTest.java
