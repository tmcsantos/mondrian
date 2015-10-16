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
public class NativeExceptMatchingTest extends BatchTestCase {

    public void testPositiveMatching() throws Exception {
        if (!MondrianProperties.instance().EnableNativeExcept.get()) {
            // No point testing these if the native filters
            // are turned off.
            return;
        }
        final String sqlPgsql =
            "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where \"store\".\"store_country\" not in('Canada')  group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC NULLS LAST";
        final String sqlMysql =
            "select `store`.`store_country` as `c0` from `store` as `store` where `store`.`store_country` not in('Canada')  group by `store`.`store_country` order by ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC";

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


    public void testNativeFilterSameAsNonNative() {
        verifySameNativeAndNot(
            "select Except([Store].[Store Name].Members, {[Store].[Canada].[BC].[Vancouver].[Store 19],[Store].[Mexico].[DF].[Mexico City].[Store 9]}) "
            + " on 0 from sales",
            "Except set.", getTestContext());
    }

    public void testCachedNativeFilter() {
        verifySameNativeAndNot(
            "select NON EMPTY Except([Store].[Store Name].Members, {[Store].[Canada].[BC].[Vancouver].[Store 19],[Store].[Mexico].[DF].[Mexico City].[Store 9]}) "
            + " on 0 from sales",
            "Except.", getTestContext());
        verifySameNativeAndNot(
            "select Except([Store].[Store Name].Members, {[Store].[Canada].[BC].[Vancouver].[Store 19],[Store].[Mexico].[DF].[Mexico City].[Store 9]}) "
            + " on 0 from sales",
            "Except.", getTestContext());
    }

    public void testMatchesWithAccessControl() {
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
