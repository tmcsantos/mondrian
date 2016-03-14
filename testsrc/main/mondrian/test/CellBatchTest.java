/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2016 Pentaho and others
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;

public class CellBatchTest extends FoodMartTestCase {

    public void testCellBatchSizeBadPerformance() {
        propSaver.set(
            MondrianProperties.instance().MaxConstraints, 10);
        propSaver.set(
            MondrianProperties.instance().CellBatchSize, 10);

        final String query =
            "with member [Measures].[c] as count(product.[product name].members, excludeempty)\n"
            + "select non empty [promotion media].children on 1,\n"
            + "non empty measures.c on 0\n"
            + "from sales";
        final String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[c]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[Bulk Mail]}\n"
            + "{[Promotion Media].[Cash Register Handout]}\n"
            + "{[Promotion Media].[Daily Paper]}\n"
            + "{[Promotion Media].[Daily Paper, Radio]}\n"
            + "{[Promotion Media].[Daily Paper, Radio, TV]}\n"
            + "{[Promotion Media].[In-Store Coupon]}\n"
            + "{[Promotion Media].[No Media]}\n"
            + "{[Promotion Media].[Product Attachment]}\n"
            + "{[Promotion Media].[Radio]}\n"
            + "{[Promotion Media].[Street Handout]}\n"
            + "{[Promotion Media].[Sunday Paper]}\n"
            + "{[Promotion Media].[Sunday Paper, Radio]}\n"
            + "{[Promotion Media].[Sunday Paper, Radio, TV]}\n"
            + "{[Promotion Media].[TV]}\n"
            + "Row #0: 919\n"
            + "Row #1: 1,184\n"
            + "Row #2: 1,261\n"
            + "Row #3: 1,195\n"
            + "Row #4: 1,337\n"
            + "Row #5: 912\n"
            + "Row #6: 1,559\n"
            + "Row #7: 1,250\n"
            + "Row #8: 628\n"
            + "Row #9: 1,088\n"
            + "Row #10: 939\n"
            + "Row #11: 1,119\n"
            + "Row #12: 719\n"
            + "Row #13: 842\n";

        Thread slowQueryThread = new Thread(
            new Runnable() {
                @Override
                public void run() {
                    executeQuery(query);
                }
            }
        );

        try {
            slowQueryThread.start();
             //give some time to query to complete. 10s should suffice.
            slowQueryThread.join(10000);
            assertEquals(
                "Possible Cell Batching cycle, query thread is still alive.",
                false, slowQueryThread.isAlive());
            assertQueryReturns(query, result);
        } catch (InterruptedException e) {
            fail();
        }
        propSaver.reset();
    }

}
// End CellBatchTest