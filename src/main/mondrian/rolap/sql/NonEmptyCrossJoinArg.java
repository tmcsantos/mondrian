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

import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.List;

public class NonEmptyCrossJoinArg implements CrossJoinArg {
    RolapLevel level;
    List<RolapMember> members;

    public NonEmptyCrossJoinArg(CrossJoinArg cjArg) {
        this.level = cjArg.getLevel();
        this.members = cjArg.getMembers();
    }

    public RolapLevel getLevel() {
        return level;
    }

    public List<RolapMember> getMembers() {
        return members;
    }

    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
        if (members != null) {
            SqlConstraintUtils.addMemberConstraint(
                sqlQuery, baseCube, aggStar,
                members, true, false, false);
        }
    }

    public boolean isPreferInterpreter(boolean joinArg) {
        return false;
    }
}
