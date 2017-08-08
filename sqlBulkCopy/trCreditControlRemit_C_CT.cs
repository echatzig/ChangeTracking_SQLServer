using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlBulkCopy
{

    class trCreditControlRemit_C_CT : ChangeTracking_Base
    {

        public override string sql_full { get { return @"
                        SELECT  

                            ID	         ,
                            ResID	     ,
                            Amount	     ,
                            Updated	     ,
                            UserID	     ,
                            ImportID	 ,
                            CleanPackage 


                        from trCreditControlRemmit_C res

            "; } }

        public override string srcTable { get { return @"trCreditControlRemmit_C"; } }
        public override string dstTable { get { return "_factCreditControlRemit_C"; } }

        public override string sql_incr { get { return @"
SELECT  
        ct.ID	         ,
        res.ResID	     ,
        res.Amount	     ,
        res.Updated	     ,
        res.UserID	     ,
        res.ImportID	 ,
        res.CleanPackage ,

		ct.SYS_CHANGE_OPERATION [Operation]

FROM CHANGETABLE(CHANGES dbo.trCreditControlRemmit_C, @StartVersionID) ct
LEFT JOIN dbo.trCreditControlRemmit_C		 res
ON  res.ID = ct.ID
WHERE (SELECT MAX(v) FROM (VALUES(ct.SYS_CHANGE_VERSION), (ct.SYS_CHANGE_CREATION_VERSION)) AS VALUE(v)) <= @EndVersionID
order by ct.SYS_CHANGE_VERSION

        "; } }

        public override int[] PKColOrdinals { get { return new int[] { 0 }; } }


    }




}
