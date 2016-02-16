/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.*;
import mondrian.server.Execution;

/**
 * Encapsulates cancel and timeouts checks
 *
 * @author Yury_Bakhmutski
 * @since Jan 18, 2016
 */
public class CancellationChecker {

    public static void checkCancelOrTimeout(
        int currentIteration, Execution execution)
    {
        int checkCancelPeriod =
            MondrianProperties.instance().CancelPhaseInterval.get();
        if (execution != null) {
            synchronized (execution) {
                if (checkCancelPeriod > 0
                    && Util.modulo(currentIteration, checkCancelPeriod) == 0)
                {
                    execution.checkCancelOrTimeout();
                }
            }
        }
    }
}
// End CancellationChecker.java