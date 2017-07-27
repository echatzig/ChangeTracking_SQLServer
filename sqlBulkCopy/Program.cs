#define ACTION_ADD_ROWS

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Z.BulkOperations;

namespace sqlBulkCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            BulkCopy b = new BulkCopy();

            b.PerformBulkCopyDifferentSchema();
            //b.PerformIncremental();

            //b.TestDelete();

        }
    }

    class BulkCopy
    {
        string srcConnStr = @"Data Source=A2DBC02;....";
        string dstConnStr = @"Data Source=....";



        string sql_full = @"
                        SELECT  
								Res.resID as BookingID, 
								ad as 'Amendments', 
								rs as 'BookingStatus',
								cs as 'Cancellations', 
								(CASE WHEN isDate(resdate) = 0 THEN null else CAST(resdate as datetime) END) as 'BookingDate', 
								restime as 'BookingTime', 
								buycur as 'BuyCurrency', 
								sellcur as 'SellCurrency', 
								exrate as 'ExchangeRate', 
								persons as 'NumberTravelled', 
								adults, 
								children, 
								infants, 
								transunit, 
								numunits, 
								costrate as 'BuyRate', 
								sellrate as 'SellRate', 
								agentcommission as 'AgentCommission', 
								agentcomperc as 'AgentCommissionPercentage',
								(case transferflag when 1 then 'Airport to Resort' when 2 then 'Resort to Airport' when 3 then 'Return' end) as 'TypeofTransfer', 
								(CASE WHEN isDate(ardate) = 0 THEN NULL else CAST(ardate as datetime) END) as 'ArrivalDate', 
								artime as 'ArrivalTime', 
								arairport as 'ArrivalAirport', 
								(CASE WHEN isDate(dedate) = 0 THEN NULL else CAST(dedate as datetime) END) as 'DepartureDate', 
								detime as 'DepartureTime', 
								deairport as 'DepartureAirport', 
								Promo_Code as 'PromoCode', 
								VAT, 
								Res.lastupdate as 'LastUpdated', 
								confirmed, 
								(case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'Hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Hoppa' end) as 'BookingOwned', 
								VehicleNameEN as 'VehicleName', 
								GBPExr as 'GBPExchangeRate', 
								HoldBookingFlag, 
								(CASE WHEN isDate(pickupdate) = 0 THEN NULL else CAST(pickupdate as datetime) END) as 'PickupDate', 
								LocFrom as 'LocationFrom', 
								LocTo as 'LocationTo',
								RemitAmount_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount), 2) FROM trCreditControlRemmit_s tCCRs WITH (NOLOCK) WHERE tCCRs.ResID = Res.ResID) ELSE 0 END),
								BookingOwned_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Ho
ppa' end) as 'BookingOwned' 
															FROM trCreditControlRemmit_s tCCRs	WITH (NOLOCK) WHERE tCCRs.ResID = Res.ResID) ELSE NULL END),
								RemitAmount_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount) ,2) FROM trCreditControlRemmit_c tCCRc WITH (NOLOCK) WHERE tCCRc.ResID = Res.ResID) ELSE 0 END),
								BookingOwned_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Hop
pa' end) as 'BookingOwned' 
															FROM trCreditControlRemmit_c tCCRc WITH (NOLOCK) WHERE tCCRc.ResID = Res.ResID) ELSE NULL END),
								Receipt					= (SELECT Top 1 rtrim(receipt) FROM Receipt_type RT WITH (NOLOCK)
															JOIN trCreditControlImports_c tCCIc WITH (NOLOCK) ON RT.id			= tCCIc.receipt_type_id 
															JOIN trCreditControl_c tCCc			WITH (NOLOCK) ON tCCc.importid	= tCCIc.importid 
															JOIN trReservations trRes			WITH (NOLOCK) ON trRes.resid		= tCCc.resid 
															where trRes.resid = Res.resid),
								Area					= (SELECT Area FROM Areas WITH (NOLOCK) WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN'),
								Country					= (SELECT Country FROM Countries C WITH (NOLOCK) JOIN Areas WITH (NOLOCK) ON C.CountryID = Areas.CountryID WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN' AND C.LangID='EN'),
								Res.fname AS FirstName,
								Res.Lname As LastName,
								Profit					= (SellRate+AmendFees-(CostRate/ExRate)-AgentCommission),
								NetAgent				= (SellRate-AgentCommission),
								GBPSell					= (SellRate+AmendFees)/GBPExr,
								GBPCommission			= (AgentCommission)/GBPExr,
								GBPProfit				= (SellRate+AmendFees-(CostRate/ExRate)-AgentCommission)/GBPExr,
								Margin					= NULL,
								OldResID as 'OldBookingID',
								Res.AgentID,
								null AS 'MidAgentID', --missing AgencyID
								null AS 'TopAgentID', --missing TAgentID 
								SupplierID AS 'PartnerID',
								Res.LinkLocID,
								ProfitMargin			= IsNull(CONVERT(DECIMAL(9,2),(SellRate+AmendFees-(CostRate/ExRate)-AgentCommission)/(Case WHEN SellRate = 0 THEN 1 ELSE sellrate END)*100, 0), 0),
								dePickupTime AS 'DeparturePickupTime',
								P2P				= (CASE WHEN HResID = '1' THEN 1 ELSE 0 END),
								CodeFrom				= (CASE WHEN CodeFrom IS NULL THEN (SELECT TOP 1 Airport from Destinations D WITH (NOLOCK) WHERE Res.AreaID = D.GAreaid) ELSE Res.CodeFrom END),
								CodeTo					= (CASE WHEN CodeTo IS NULL THEN (SELECT TOP 1 RHCode from Destinations D WITH (NOLOCK)
															JOIN trLinkLocations	AS tLL WITH (NOLOCK) ON tLL.ResortID = D.AreaID
															JOIN trPrices			AS tP  WITH (NOLOCK) ON tp.LinkLocID = tll.LinkLocID
															WHERE Res.PriceID = tp.ID 
															) ELSE Res.CodeTo END),
                                null as BookingDateYear,
								Res.InvoiceID,
								Res.PriceID,
								Res.UnitID,
								TransferType	= (select VehicleType=
													case
													 when res.unitid is not null then
													  (select Transfertype from trTransportation a WITH (NOLOCK) WHERE a.UnitID=res.UnitID) 
													 when res.unitid is null and booking_owned=0 and VehicleNameEN <>'' then
													  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
													 when res.unitid is null and booking_owned=1 and VehicleNameEN <>'' then
													  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
													 when res.unitid is null and booking_owned=0 and (VehicleNameEN='' or VehicleNameEN is null) then
													  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE a.type like '%' + LTRIM(RTRIM(Transunit)) + '%') 
													 when res.unitid is null and booking_owned=1 and (VehicleNameEN='' or VehicleNameEN is null)  then
													  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(Transunit)) + '%' )) 
													end),
								res.EMail,
								NULL as OriginalBookingDate,
								Mobile

						from trreservations res WITH (NOLOCK)

            ";

        public void PerformBulkCopyDifferentSchema()
        {
            Int64 vStartVersionID;


            // DataTable sourceData = new DataTable();
            // get the source data
            using (SqlConnection srcConn =
                            new SqlConnection(srcConnStr))
            {
                srcConn.Open();

                //----

                var fatUglyTransaction = srcConn.BeginTransaction(IsolationLevel.Serializable);

                //----

                string sql3 = @"select EndVersionID = CHANGE_TRACKING_CURRENT_VERSION()";
                SqlCommand sqlCmd3 = new SqlCommand(sql3, srcConn, fatUglyTransaction);
                vStartVersionID = (Int64)sqlCmd3.ExecuteScalar();

                //----

                SqlCommand cmdGetCT1 =
                    new SqlCommand(@"
                        insert into [etl].[Change_Tracking_Version] (Table_Name, Change_Tracking_Version)
                        select 'trReservations', @StartVersionID
                        ", srcConn, fatUglyTransaction);
                cmdGetCT1.Parameters.AddWithValue("@StartVersionID", vStartVersionID);
                cmdGetCT1.ExecuteNonQuery();

                //----

                SqlCommand myCommand =
                    new SqlCommand(sql_full, srcConn, fatUglyTransaction);



                SqlDataReader reader = myCommand.ExecuteReader();
                // open the destination data
                using (SqlConnection dstConn =
                            new SqlConnection(dstConnStr))
                {
                    // open the connection
                    dstConn.Open();
                    using (SqlBulkCopy bulkCopy =
                        new SqlBulkCopy(dstConn.ConnectionString))
                    {
                        bulkCopy.BatchSize = 500;
                        bulkCopy.NotifyAfter = 1000;
                        bulkCopy.SqlRowsCopied +=
                            new SqlRowsCopiedEventHandler(bulkCopy_SqlRowsCopied);

                        //bulkCopy.ColumnMappings.Add("ProductID", "ProductID");
                        //bulkCopy.ColumnMappings.Add("ProductName", "Name");
                        //bulkCopy.ColumnMappings.Add("QuantityPerUnit", "Quantity");
                        bulkCopy.DestinationTableName = "_factBookings";
                        bulkCopy.WriteToServer(reader);
                    }
                }
                reader.Close();

                // ----
                string sqlCT_2 = @"select EndVersionID = CHANGE_TRACKING_CURRENT_VERSION()";
                SqlCommand cmdCT_2 = new SqlCommand(sqlCT_2, srcConn, fatUglyTransaction);
                Int64 vStartVersionID_2 = (Int64)cmdCT_2.ExecuteScalar();

                Debug.Assert(vStartVersionID == vStartVersionID_2);

                // ----
                fatUglyTransaction.Commit();

            }
        }


        string sql_incr = @"
SELECT  
		Res.resID as BookingID, 
		res.ad as 'Amendments', 
		res.rs as 'BookingStatus',
		res.cs as 'Cancellations', 
		(CASE WHEN isDate(resdate) = 0 THEN null else CAST(resdate as datetime) END) as 'BookingDate', 
		restime as 'BookingTime', 
		buycur as 'BuyCurrency', 
		sellcur as 'SellCurrency', 
		exrate as 'ExchangeRate', 
		persons as 'NumberTravelled', 
		adults, 
		children, 
		infants, 
		transunit, 
		numunits, 
		costrate as 'BuyRate', 
		sellrate as 'SellRate', 
		agentcommission as 'AgentCommission', 
		agentcomperc as 'AgentCommissionPercentage',
		(case transferflag when 1 then 'Airport to Resort' when 2 then 'Resort to Airport' when 3 then 'Return' end) as 'TypeofTransfer', 
		(CASE WHEN isDate(ardate) = 0 THEN NULL else CAST(ardate as datetime) END) as 'ArrivalDate', 
		artime as 'ArrivalTime', 
		arairport as 'ArrivalAirport', 
		(CASE WHEN isDate(dedate) = 0 THEN NULL else CAST(dedate as datetime) END) as 'DepartureDate', 
		detime as 'DepartureTime', 
		deairport as 'DepartureAirport', 
		Promo_Code as 'PromoCode', 
		VAT, 
		Res.lastupdate as 'LastUpdated', 
		confirmed, 
		(case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'Hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Hoppa' end) as 'BookingOwned', 
		VehicleNameEN as 'VehicleName', 
		GBPExr as 'GBPExchangeRate', 
		HoldBookingFlag, 
		(CASE WHEN isDate(pickupdate) = 0 THEN NULL else CAST(pickupdate as datetime) END) as 'PickupDate', 
		LocFrom as 'LocationFrom', 
		LocTo as 'LocationTo',
		RemitAmount_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount), 2) FROM trCreditControlRemmit_s tCCRs WITH (NOLOCK) WHERE tCCRs.ResID = Res.ResID) ELSE 0 END),
		BookingOwned_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Ho
ppa' end) as 'BookingOwned' 
									FROM trCreditControlRemmit_s tCCRs	WITH (NOLOCK) WHERE tCCRs.ResID = Res.ResID) ELSE NULL END),
		RemitAmount_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount) ,2) FROM trCreditControlRemmit_c tCCRc WITH (NOLOCK) WHERE tCCRc.ResID = Res.ResID) ELSE 0 END),
		BookingOwned_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Hop
pa' end) as 'BookingOwned' 
									FROM trCreditControlRemmit_c tCCRc WITH (NOLOCK) WHERE tCCRc.ResID = Res.ResID) ELSE NULL END),
		Receipt					= (SELECT Top 1 rtrim(receipt) FROM Receipt_type RT WITH (NOLOCK)
									JOIN trCreditControlImports_c tCCIc WITH (NOLOCK) ON RT.id			= tCCIc.receipt_type_id 
									JOIN trCreditControl_c tCCc			WITH (NOLOCK) ON tCCc.importid	= tCCIc.importid 
									JOIN trReservations trRes			WITH (NOLOCK) ON trRes.resid		= tCCc.resid 
									where trRes.resid = Res.resid),
		Area					= (SELECT Area FROM Areas WITH (NOLOCK) WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN'),
		Country					= (SELECT Country FROM Countries C WITH (NOLOCK) JOIN Areas WITH (NOLOCK) ON C.CountryID = Areas.CountryID WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN' AND C.LangID='EN'),
		Res.fname AS FirstName,
		Res.Lname As LastName,
		Profit					= (SellRate+AmendFees-(CostRate/ExRate)-AgentCommission),
		NetAgent				= (SellRate-AgentCommission),
		GBPSell					= (SellRate+AmendFees)/GBPExr,
		GBPCommission			= (AgentCommission)/GBPExr,
		GBPProfit				= (SellRate+AmendFees-(CostRate/ExRate)-AgentCommission)/GBPExr,
		Margin					= NULL,
		OldResID as 'OldBookingID',
		Res.AgentID,
		null AS 'MidAgentID', --missing AgencyID
		null AS 'TopAgentID', --missing TAgentID 
		SupplierID AS 'PartnerID',
		Res.LinkLocID,
		ProfitMargin			= IsNull(CONVERT(DECIMAL(9,2),(SellRate+AmendFees-(CostRate/ExRate)-AgentCommission)/(Case WHEN SellRate = 0 THEN 1 ELSE sellrate END)*100, 0), 0),
		dePickupTime AS 'DeparturePickupTime',
		P2P				= (CASE WHEN HResID = '1' THEN 1 ELSE 0 END),
		CodeFrom				= (CASE WHEN CodeFrom IS NULL THEN (SELECT TOP 1 Airport from Destinations D WITH (NOLOCK) WHERE Res.AreaID = D.GAreaid) ELSE Res.CodeFrom END),
		CodeTo					= (CASE WHEN CodeTo IS NULL THEN (SELECT TOP 1 RHCode from Destinations D WITH (NOLOCK)
									JOIN trLinkLocations	AS tLL WITH (NOLOCK) ON tLL.ResortID = D.AreaID
									JOIN trPrices			AS tP  WITH (NOLOCK) ON tp.LinkLocID = tll.LinkLocID
									WHERE Res.PriceID = tp.ID 
									) ELSE Res.CodeTo END),
		null as BookingDateYear,
		Res.InvoiceID,
		Res.PriceID,
		Res.UnitID,
		TransferType	= (select VehicleType=
							case
							 when res.unitid is not null then
							  (select Transfertype from trTransportation a WITH (NOLOCK) WHERE a.UnitID=res.UnitID) 
							 when res.unitid is null and booking_owned=0 and VehicleNameEN <>'' then
							  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
							 when res.unitid is null and booking_owned=1 and VehicleNameEN <>'' then
							  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
							 when res.unitid is null and booking_owned=0 and (VehicleNameEN='' or VehicleNameEN is null) then
							  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE a.type like '%' + LTRIM(RTRIM(Transunit)) + '%') 
							 when res.unitid is null and booking_owned=1 and (VehicleNameEN='' or VehicleNameEN is null)  then
							  (select top 1 Transfertype from trTransportation a WITH (NOLOCK) WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(Transunit)) + '%' )) 
							end),
		res.EMail,
		NULL as OriginalBookingDate,
		Mobile,
		
		ct.SYS_CHANGE_OPERATION [Operation]

FROM CHANGETABLE(CHANGES dbo.trReservations, @StartVersionID) ct
LEFT JOIN dbo.trReservations res
ON  res.ResID = ct.ResID
and res.RS=ct.RS
and res.CS=ct.CS
and res.AD=ct.AD
WHERE (SELECT MAX(v) FROM (VALUES(ct.SYS_CHANGE_VERSION), (ct.SYS_CHANGE_CREATION_VERSION)) AS VALUE(v)) <= @EndVersionID

        ";

        public void PerformIncremental()
        {
            Int64 vStartVersionID;
            Int64 vEndVersionID;

            // DataTable sourceData = new DataTable();
            // get the source data
            using (SqlConnection srcConn =
                            new SqlConnection(srcConnStr))
            {
                srcConn.Open();

                string sql2 = @"SELECT Change_Tracking_Version
                    FROM etl.Change_Tracking_Version
                    WHERE Table_Name = 'trReservations'";
                SqlCommand sqlCmd2 = new SqlCommand(sql2, srcConn);
                vStartVersionID = (Int64)sqlCmd2.ExecuteScalar();

                string sql3 = @"select EndVersionID = CHANGE_TRACKING_CURRENT_VERSION()";
                SqlCommand sqlCmd3 = new SqlCommand(sql3, srcConn);
                vEndVersionID = (Int64)sqlCmd3.ExecuteScalar();


                SqlCommand myCommand =
                    new SqlCommand(sql_incr, srcConn);
                myCommand.Parameters.AddWithValue("@StartVersionID", vStartVersionID);
                myCommand.Parameters.AddWithValue("@EndVersionID", vEndVersionID);

                SqlDataReader reader = myCommand.ExecuteReader();
                DataTable dt = new DataTable();
                for(var c =0; c < reader.FieldCount; c++)
                {
                    dt.Columns.Add(reader.GetName(c),reader.GetFieldType(c));
                }

                // index of last field (set to OPERATION)
                int fldOPER = reader.FieldCount - 1;  

                object[]obj = new object[reader.FieldCount];

#if ACTION_ADD_ROWS
                // add rows

                while(reader.Read())
                {
                    reader.GetValues(obj);

                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    ////     continue;
                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    ////     continue;

                    var dr = dt.Rows.Add(obj);

                    if (reader.GetSqlChars(fldOPER).Value[0] == 'I')
                        /* no-op */
                    if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    {
                        dr.AcceptChanges();
                        dr.Delete();
                    }
                    if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    {
                      dr.AcceptChanges();
                      dr.SetModified();
                    }
                }

#else
                // remove rows
                while (reader.Read())
                {
                    reader.GetValues(obj);

                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    ////     continue;
                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    ////     continue;

                    var dr = dt.Rows.Add(obj);

                    // delete I,U rows 
                    dr.AcceptChanges();
                    dr.Delete();
                }
#endif


                SqlConnection dstConn =
                            new SqlConnection(dstConnStr);
                dstConn.Open();

                SqlDataAdapter da = new SqlDataAdapter("select * from _factBookings", dstConn);
                var sqlCB = new SqlCommandBuilder(da);

                SqlCommand delCmd = new SqlCommand(@"
                    delete from _factBookings 
                    where 
                            BookingID       = @BookingID
                    and     Amendments      = @Amendments   
                    and     BookingStatus   = @BookingStatus
                    and     Cancellations   = @Cancellations
                ", dstConn);

                delCmd.Parameters.Add(new SqlParameter("@BookingID", SqlDbType.VarChar));
                delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                delCmd.Parameters["@BookingID"].SourceColumn = "BookingID";

                delCmd.Parameters.Add(new SqlParameter("@Amendments", SqlDbType.TinyInt));
                delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                delCmd.Parameters["@Amendments"].SourceColumn = "Amendments";

                delCmd.Parameters.Add(new SqlParameter("@BookingStatus", SqlDbType.TinyInt));
                delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                delCmd.Parameters["@BookingStatus"].SourceColumn = "BookingStatus";

                delCmd.Parameters.Add(new SqlParameter("@Cancellations", SqlDbType.TinyInt));
                delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                delCmd.Parameters["@Cancellations"].SourceColumn = "Cancellations";

                da.DeleteCommand = delCmd;

                Console.WriteLine(da.DeleteCommand.CommandText);

                da.Update(dt);

                reader.Close();

                dstConn.Close();

                // ----

                SqlCommand cmdSet_CT =
                    new SqlCommand(@"
                        update [etl].[Change_Tracking_Version] 
                        set Change_Tracking_Version = @EndVersionID
                        where Table_Name= 'trReservations'
                        ", srcConn);
                cmdSet_CT.Parameters.AddWithValue("@EndVersionID", vEndVersionID);
                cmdSet_CT.ExecuteNonQuery();

            }
        }

        void bulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            System.Console.WriteLine("rows copied:" + e.RowsCopied);
        }


        static DataTable GetTable()
        {
            // Here we create a DataTable with four columns.
            DataTable table = new DataTable();
            table.Columns.Add("Weight", typeof(int));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("Breed", typeof(string));
            table.Columns.Add("Date", typeof(DateTime));

            // Here we add five DataRows.
            table.Rows.Add(57, "Koko", "Shar Pei",
                DateTime.Now);
            table.Rows.Add(130, "Fido", "Bullmastiff",
                DateTime.Now);
            table.Rows.Add(92, "Alex", "Anatolian Shepherd Dog",
                DateTime.Now);
            table.Rows.Add(25, "Charles", "Cavalier King Charles Spaniel",
                DateTime.Now);
            table.Rows.Add(7, "Candy", "Yorkshire Terrier",
                DateTime.Now);
            return table;
        }

        internal void TestDelete()
        {
               //
                // Get the first row for the DataTable
                //
                DataTable table = GetTable();

            table.AcceptChanges();


            DataRow row = table.Rows[0];
            //
            // Delete the first row.
            // ... This means the second row is the first row.
            //

                row.Delete();
                //
                // Display the new first row.
                //
                row = table.Rows[0];
                Console.WriteLine(row["Name", DataRowVersion.Original]);
        }
    }
}
