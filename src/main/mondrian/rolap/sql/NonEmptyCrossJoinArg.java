/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015 Pentaho
// All Rights Reserved.
*/


package mondrian.rolap.sql;

import mondrian.olap.MondrianDef;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;

import java.util.*;

public class NonEmptyCrossJoinArg implements CrossJoinArg {
    RolapLevel level;
    List<RolapMember> members;
    RolapMember member;

    public NonEmptyCrossJoinArg(CrossJoinArg cjArg, RolapMember member) {
        this.level = cjArg.getLevel();
        this.members = cjArg.getMembers();
        this.member = member;
    }

    public RolapLevel getLevel() {
        return level;
    }

    public List<RolapMember> getMembers() {
        return members;
    }

    private String compile(
        SqlQuery sqlQuery,
        AggStar aggStar)
    {
        if (!(member instanceof RolapStoredMeasure)) {
            return null;
        }
        RolapStoredMeasure measure = (RolapStoredMeasure) member;
        if (measure.isCalculated()) {
            return null;
        }
        RolapAggregator aggregator = measure.getAggregator();
        String exprInner;

        // Use aggregate table to create condition if available
        if (aggStar != null
            && measure.getStarMeasure() instanceof RolapStar.Column)
        {
            RolapStar.Column column =
                (RolapStar.Column) measure.getStarMeasure();
            int bitPos = column.getBitPosition();
            AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
            exprInner = aggColumn.generateExprString(sqlQuery);
            if (aggColumn instanceof AggStar.FactTable.Measure) {
                RolapAggregator aggTableAggregator =
                    ((AggStar.FactTable.Measure) aggColumn)
                        .getAggregator();
                // aggregating data that has already been aggregated
                // should be done with another aggregators
                // e.g., counting facts should be proceeded via computing
                // sum, as a row can aggregate several facts
                aggregator = (RolapAggregator) aggTableAggregator
                    .getRollup();
            }
        } else {
            MondrianDef.Expression defExp =
                measure.getMondrianDefExpression();
            exprInner = (defExp == null)
                ? "*" : defExp.getExpression(sqlQuery);
        }

        String expr = aggregator.getExpression(exprInner);
        if (sqlQuery.getDialect().getDatabaseProduct().getFamily()
            == Dialect.DatabaseProduct.DB2)
        {
            expr = "FLOAT(" + expr + ")";
        }
        return "NOT((" + expr + " is null))";
    }

    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
        if (member != null) {
            String filterSql = compile(sqlQuery, aggStar);
            if (filterSql != null) {
                sqlQuery.addHaving(filterSql);
            }
        }
    }

    public boolean isPreferInterpreter(boolean joinArg) {
        return false;
    }

    private boolean equals(Object o1, Object o2) {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof NonEmptyCrossJoinArg)) {
            return false;
        }
        NonEmptyCrossJoinArg that = (NonEmptyCrossJoinArg) obj;
        if (!equals(this.level, that.level)) {
            return false;
        }

        if (!equals(this.member, that.member)) {
            return false;
        }

        if (members != null && that.members != null) {
            if (members.size() != that.members.size()) {
                return false;
            }
        }

        return equals(members, that.members);
    }

    public int hashCode() {
        int c = 12;
        if (level != null) {
            c = level.hashCode();
        }
        if (member != null) {
            c = 31 * c + member.hashCode();
        }
        if (members != null) {
            for (RolapMember member : members) {
                c = 31 * c + member.hashCode();
            }
        }
        return c;
    }
}
