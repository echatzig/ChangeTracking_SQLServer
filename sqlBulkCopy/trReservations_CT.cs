﻿#define ACTION_ADD_ROWS

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

    class trReservations_CT : ChangeTracking_Base
    {

        public override string sql_full { get { return @"
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
								RemitAmount_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount), 2) FROM trCreditControlRemmit_s tCCRs  WHERE tCCRs.ResID = Res.ResID) ELSE 0 END),
								BookingOwned_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Ho
ppa' end) as 'BookingOwned' 
															FROM trCreditControlRemmit_s tCCRs	 WHERE tCCRs.ResID = Res.ResID) ELSE NULL END),
								RemitAmount_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount) ,2) FROM trCreditControlRemmit_c tCCRc  WHERE tCCRc.ResID = Res.ResID) ELSE 0 END),
								BookingOwned_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Hop
pa' end) as 'BookingOwned' 
															FROM trCreditControlRemmit_c tCCRc  WHERE tCCRc.ResID = Res.ResID) ELSE NULL END),
								Receipt					= (SELECT Top 1 rtrim(receipt) FROM Receipt_type RT 
															JOIN trCreditControlImports_c tCCIc  ON RT.id			= tCCIc.receipt_type_id 
															JOIN trCreditControl_c tCCc			 ON tCCc.importid	= tCCIc.importid 
															JOIN trReservations trRes			 ON trRes.resid		= tCCc.resid 
															where trRes.resid = Res.resid),
								Area					= (SELECT Area FROM Areas  WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN'),
								Country					= (SELECT Country FROM Countries C  JOIN Areas  ON C.CountryID = Areas.CountryID WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN' AND C.LangID='EN'),
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
								CodeFrom				= (CASE WHEN CodeFrom IS NULL THEN (SELECT TOP 1 Airport from Destinations D  WHERE Res.AreaID = D.GAreaid) ELSE Res.CodeFrom END),
								CodeTo					= (CASE WHEN CodeTo IS NULL THEN (SELECT TOP 1 RHCode from Destinations D 
															JOIN trLinkLocations	AS tLL  ON tLL.ResortID = D.AreaID
															JOIN trPrices			AS tP   ON tp.LinkLocID = tll.LinkLocID
															WHERE Res.PriceID = tp.ID 
															) ELSE Res.CodeTo END),
                                null as BookingDateYear,
								Res.InvoiceID,
								Res.PriceID,
								Res.UnitID,
								TransferType	= (select VehicleType=
													case
													 when res.unitid is not null then
													  (select Transfertype from trTransportation a  WHERE a.UnitID=res.UnitID) 
													 when res.unitid is null and booking_owned=0 and VehicleNameEN <>'' then
													  (select top 1 Transfertype from trTransportation a  WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
													 when res.unitid is null and booking_owned=1 and VehicleNameEN <>'' then
													  (select top 1 Transfertype from trTransportation a  WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
													 when res.unitid is null and booking_owned=0 and (VehicleNameEN='' or VehicleNameEN is null) then
													  (select top 1 Transfertype from trTransportation a  WHERE a.type like '%' + LTRIM(RTRIM(Transunit)) + '%') 
													 when res.unitid is null and booking_owned=1 and (VehicleNameEN='' or VehicleNameEN is null)  then
													  (select top 1 Transfertype from trTransportation a  WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(Transunit)) + '%' )) 
													end),
								res.EMail,
								NULL as OriginalBookingDate,
								Mobile

						from trreservations res

            "; } }

        public override string srcTable { get { return "trReservations" ; } }
        public override string dstTable { get { return "_factBookings"; } }

        public override string sql_incr { get { return @"
SELECT  
		ct.resID as BookingID, 
		ct.ad as 'Amendments', 
		ct.rs as 'BookingStatus',
		ct.cs as 'Cancellations', 
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
		RemitAmount_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount), 2) FROM trCreditControlRemmit_s tCCRs  WHERE tCCRs.ResID = Res.ResID) ELSE 0 END),
		BookingOwned_Supplier	= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Ho
ppa' end) as 'BookingOwned' 
									FROM trCreditControlRemmit_s tCCRs	 WHERE tCCRs.ResID = Res.ResID) ELSE NULL END),
		RemitAmount_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT ROUND(SUM(Amount) ,2) FROM trCreditControlRemmit_c tCCRc  WHERE tCCRc.ResID = Res.ResID) ELSE 0 END),
		BookingOwned_Client		= (CASE WHEN Res.RS=1 AND Res.CS IN (0, 1) THEN (SELECT Top 1 (case when booking_owned = 0 then 'A2B' when booking_owned = 1 and Res.agentid = 60042 then 'hoppa' when booking_owned = 1 and Res.agentid <> 60042 then 'Resort Hop
pa' end) as 'BookingOwned' 
									FROM trCreditControlRemmit_c tCCRc  WHERE tCCRc.ResID = Res.ResID) ELSE NULL END),
		Receipt					= (SELECT Top 1 rtrim(receipt) FROM Receipt_type RT 
									JOIN trCreditControlImports_c tCCIc  ON RT.id			= tCCIc.receipt_type_id 
									JOIN trCreditControl_c tCCc			 ON tCCc.importid	= tCCIc.importid 
									JOIN trReservations trRes			 ON trRes.resid		= tCCc.resid 
									where trRes.resid = Res.resid),
		Area					= (SELECT Area FROM Areas  WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN'),
		Country					= (SELECT Country FROM Countries C  JOIN Areas  ON C.CountryID = Areas.CountryID WHERE Areas.GAreaID = Res.AreaID AND Areas.LangID = 'EN' AND C.LangID='EN'),
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
		CodeFrom				= (CASE WHEN CodeFrom IS NULL THEN (SELECT TOP 1 Airport from Destinations D  WHERE Res.AreaID = D.GAreaid) ELSE Res.CodeFrom END),
		CodeTo					= (CASE WHEN CodeTo IS NULL THEN (SELECT TOP 1 RHCode from Destinations D 
									JOIN trLinkLocations	AS tLL  ON tLL.ResortID = D.AreaID
									JOIN trPrices			AS tP   ON tp.LinkLocID = tll.LinkLocID
									WHERE Res.PriceID = tp.ID 
									) ELSE Res.CodeTo END),
		null as BookingDateYear,
		Res.InvoiceID,
		Res.PriceID,
		Res.UnitID,
		TransferType	= (select VehicleType=
							case
							 when res.unitid is not null then
							  (select Transfertype from trTransportation a  WHERE a.UnitID=res.UnitID) 
							 when res.unitid is null and booking_owned=0 and VehicleNameEN <>'' then
							  (select top 1 Transfertype from trTransportation a  WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
							 when res.unitid is null and booking_owned=1 and VehicleNameEN <>'' then
							  (select top 1 Transfertype from trTransportation a  WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(res.VehicleNameEN)) + '%')) 
							 when res.unitid is null and booking_owned=0 and (VehicleNameEN='' or VehicleNameEN is null) then
							  (select top 1 Transfertype from trTransportation a  WHERE a.type like '%' + LTRIM(RTRIM(Transunit)) + '%') 
							 when res.unitid is null and booking_owned=1 and (VehicleNameEN='' or VehicleNameEN is null)  then
							  (select top 1 Transfertype from trTransportation a  WHERE VehicleCode in (SELECT VehicleCode FROM trRHVehicles where lang='en' and Vehicle like '%' + LTRIM(RTRIM(Transunit)) + '%' )) 
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
order by ct.SYS_CHANGE_VERSION

        "; } }

        public override int[] PKColOrdinals { get { return new int[] { 0,1,2,3 }; } }


    }


}
