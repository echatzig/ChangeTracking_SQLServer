#define ACTION_ADD_ROWS

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


/*
 * https://logicalread.com/sql-server-change-tracking-bulletproof-etl-p1-mb01/#.WX8U_oiGOCg
 */
namespace sqlBulkCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            // BulkCopy b = new BulkCopy();
            // //
            // ////b.PerformBulkCopyDifferentSchema();
            // ////
            // //// we need to find a way to trigger U,D since we currently treat them as I !!
            // b.PerformIncremental();
            //
            // //b.TestDelete();
            // //b.testSqlCommandBuilder();
            //
            // BulkCopy_CCRemit_S bb = new BulkCopy_CCRemit_S();
            // //bb.PerformBulkCopyDifferentSchema();
            // bb.PerformIncremental();

            //BulkCopy_Azure_to_dbc02 b = new BulkCopy_Azure_to_dbc02();
            //b.PerformBulkCopyDifferentSchema();
            //

            //var b = new trCreditControlRemit_C_CT();

            var b1 = new trReservations_CT();

            // b.PerformBulkCopyDifferentSchema();
            b1.PerformIncremental();

            var b2 = new trCreditControlRemit_C_CT();
            b2.PerformIncremental();

            var b3 = new trCreditControlRemit_S_CT();
            b3.PerformIncremental();


        }
    }


 
}
