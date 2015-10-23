/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation.  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.Dialect;
import mondrian.test.*;

/**
 * Test case for pushing MDX except conditions down to SQL.
 */
public class NativeExceptTest extends BatchTestCase {

    public void testWithCJ() throws Exception {
        propSaver.set(MondrianProperties.instance().EnableNativeExcept, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final boolean requestFreshConnection = true;
        final String query =
            "select non empty crossjoin([Store].[Store Country].Members, except(time.month.members, {time.[1997].q1.[1], time.[1997].q1.[2]})) on 0 from sales";

        checkNative(100, 10, query, null, requestFreshConnection);
    }

    public void testWithFilters() throws Exception {
        final String time =
            "with \n"
            + "set [~dates] as 'nonempty(time.month.members)'\n"
            + "select except([~dates], {time.[1997].q1.[1], time.[1997].q1.[2]}) on 0 from sales";
        verifySameNativeAndNot(time, null, getTestContext());

        final String store =
            "select except([Store].[USA].Children, {[Store].[USA].[CA], [Store].[USA].[OR]}) on 0 from sales";
        verifySameNativeAndNot(store, null, getTestContext());
    }

    public void testNativeExcept() {
        propSaver.set(MondrianProperties.instance().EnableNativeExcept, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final boolean requestFreshConnection = true;
        final String query =
            "select except(time.month.members, {time.[1997].q1.[1], time.[1997].q1.[2]}) on 0 from sales";

        checkNative(100, 22, query, null, requestFreshConnection);
    }

    public void testNativeExceptWithNE() {
        propSaver.set(MondrianProperties.instance().EnableNativeExcept, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeNonEmptyFun, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final boolean requestFreshConnection = true;

        String query =
            "With \n"
            + "Set [*NATIVE_EXCEPT] as 'Except(Cache([*NATIVE_NE_SET]), {[Time].[1997].[Q1].[1], [Time].[1997].[Q1].[2]})'\n"
            + "Set [*NATIVE_NE_SET] as 'NonEmpty([Time].[Month].members)'\n"
            + "Select \n"
            + "[*NATIVE_EXCEPT] on columns \n"
            + "From [Sales]";

        checkNative(100, 10, query, null, requestFreshConnection);
    }

    public void testHierarchies() throws Exception {
        if (!MondrianProperties.instance().EnableNativeExcept.get()) {
            // No point testing these if the native filters
            // are turned off.
            return;
        }
        final String query =
            "select except(time.month.members, {time.[1997].q1.[1], time.[1997].q1.[2]}) on 0 from sales";

        final String sqlMysql =
            "select `time_by_day`.`the_year` as `c0`, `time_by_day`.`quarter` as `c1`, `time_by_day`.`month_of_year` as `c2` from `time_by_day` as `time_by_day` where ((not ((`time_by_day`.`month_of_year`, `time_by_day`.`quarter`, `time_by_day`.`the_year`) in ((1, 'Q1', 1997))) or (`time_by_day`.`month_of_year` is null or `time_by_day`.`quarter` is null or `time_by_day`.`the_year` is null)) and (not ((`time_by_day`.`month_of_year`, `time_by_day`.`quarter`, `time_by_day`.`the_year`) in ((2, 'Q1', 1997))) or (`time_by_day`.`month_of_year` is null or `time_by_day`.`quarter` is null or `time_by_day`.`the_year` is null)))  group by `time_by_day`.`the_year`, `time_by_day`.`quarter`, `time_by_day`.`month_of_year` order by ISNULL(`time_by_day`.`the_year`) ASC, `time_by_day`.`the_year` ASC, ISNULL(`time_by_day`.`quarter`) ASC, `time_by_day`.`quarter` ASC, ISNULL(`time_by_day`.`month_of_year`) ASC, `time_by_day`.`month_of_year` ASC";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysql,
                sqlMysql.length())
        };

        assertQuerySqlOrNot(
            getTestContext(),
            query,
            patterns,
            false,
            true,
            true);
        verifySameNativeAndNot(query, "with hierarchies", getTestContext());
    }

    public void testPositiveMatching() throws Exception {
        if (!MondrianProperties.instance().EnableNativeExcept.get()) {
            // No point testing these if the native filters
            // are turned off.
            return;
        }
        final String sqlPgsql =
            "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where \"store\".\"store_country\" not in('Canada')  group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC NULLS LAST";
        final String sqlMysql =
            "select `store`.`store_country` as `c0` from `store` as `store` where ((not ((`store`.`store_country`) in (('Canada'))) or (`store`.`store_country` is null)))  group by `store`.`store_country` order by ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysql,
                sqlMysql.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.POSTGRESQL,
                sqlPgsql,
                sqlPgsql.length())
        };
        final String queryResults =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Mexico], [Measures].[Unit Sales]}\n"
            + "{[Store].[USA], [Measures].[Unit Sales]}\n"
            + "Row #0: \n"
            + "Row #0: 266,773\n";
        final String query =
            "select crossjoin(except(store.[Store Country].members, {[Store].[Canada]}), measures.[Unit Sales]) on 0 from sales";
        assertQuerySqlOrNot(
            getTestContext(),
            query,
            patterns,
            false,
            true,
            true);
        assertQueryReturns(
            query,
            queryResults);
        verifySameNativeAndNot(query, null, getTestContext());
    }

    public void testNativeExceptSameAsNonNative() {
        verifySameNativeAndNot(
            "select Except([Store].[Store Name].Members, {[Store].[Canada].[BC].[Vancouver].[Store 19],[Store].[Mexico].[DF].[Mexico City].[Store 9]}) "
            + " on 0 from sales",
            "Except set.", getTestContext());
    }

    public void testCachedNativeExcept() {
        verifySameNativeAndNot(
            "select NON EMPTY Except([Store].[Store Name].Members, {[Store].[Canada].[BC].[Vancouver].[Store 19],[Store].[Mexico].[DF].[Mexico City].[Store 9]}) "
            + " on 0 from sales",
            "Except.", getTestContext());
        verifySameNativeAndNot(
            "select Except([Store].[Store Name].Members, {[Store].[Canada].[BC].[Vancouver].[Store 19],[Store].[Mexico].[DF].[Mexico City].[Store 9]}) "
            + " on 0 from sales",
            "Except.", getTestContext());
    }

    public void testWithAccessControl() {
        String dimension =
            "<Dimension name=\"Store2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\"  >\n"
            + "    <Table name=\"store\"/>\n"
            + "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"TinySales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Store2\" source=\"Store2\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";


        final String roleDefs =
            "<Role name=\"test\">\n"
            + "        <SchemaGrant access=\"none\">\n"
            + "            <CubeGrant cube=\"TinySales\" access=\"all\">\n"
            + "                <HierarchyGrant hierarchy=\"[Store2]\" access=\"custom\"\n"
            + "                                 rollupPolicy=\"PARTIAL\">\n"
            + "                    <MemberGrant member=\"[Store2].[USA].[CA]\" access=\"all\"/>\n"
            + "                    <MemberGrant member=\"[Store2].[USA].[OR]\" access=\"all\"/>\n"
            + "                    <MemberGrant member=\"[Store2].[Canada]\" access=\"all\"/>\n"
            + "                </HierarchyGrant>\n"
            + "            </CubeGrant>\n"
            + "        </SchemaGrant>\n"
            + "    </Role> ";

        final TestContext context = getTestContext().create(
            dimension,
            cube, null, null, null,
            roleDefs).withRole("test");
        verifySameNativeAndNot(
            "select Except([Product].[Product Category].Members, {[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]})"
            + " on 0 from tinysales",
            "Except on dim with full access.", context);
        verifySameNativeAndNot(
            "select Except([Store2].[USA].Children, {[Store2].[USA].[CA], [Store2].[USA].[OR]})"
            + " on 0 from tinysales",
            "Except on restricted dimension.  Should be empty set.", context);
    }
}

// End NativeExceptMatchingTest.java
