#define ACTION_ADD_ROWS

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace sqlBulkCopy
{

    abstract class ChangeTracking_Base
    {
        string srcConnStr = ConfigurationManager.ConnectionStrings["a2dbc02"].ConnectionString;
        string dstConnStr = ConfigurationManager.ConnectionStrings["azure"].ConnectionString;



        public abstract string sql_full { get; }

        public abstract string srcTable { get; }
        public abstract string dstTable { get; }

        public abstract string sql_incr { get; }

        public abstract int[] PKColOrdinals { get; }




        /*
            --- enable

            alter database Unique_Golf set change_tracking=ON (change_retention=14 days, auto_cleanup = on)

            alter table trReservations enable change_tracking with (track_columns_updated=off)

            grant view change tracking on trReservations to transfer

            --- disable
            alter table trReservations disable change_tracking 

            alter database Unique_Golf set change_tracking=OFF

        */
        public void PerformBulkCopyDifferentSchema()
        {
            Int64 vStartVersionID;


            // DataTable sourceData = new DataTable();
            // get the source data
            using (SqlConnection srcConn =
                            new SqlConnection(srcConnStr))
            {
                srcConn.Open();

                try
                {
                    string sql_1 = "alter database Unique_Golf set change_tracking = ON(change_retention = 14 days, auto_cleanup = on)";
                    execNonQuery(srcConn, sql_1);
                }
                catch (SqlException ex) { Console.WriteLine(ex.Message); }

                try { 
                    string sql_2 = "alter table " + srcTable + " enable change_tracking with (track_columns_updated = off)";
                    execNonQuery(srcConn, sql_2);
                }
                catch (SqlException ex) { Console.WriteLine(ex.Message); }

                try { 
                    string sql_3 = "grant view change tracking on " + srcTable + " to transfer";
                    execNonQuery(srcConn, sql_3);
                }
                catch (SqlException ex) { Console.WriteLine(ex.Message); }

                //----

                var fatUglyTransaction = srcConn.BeginTransaction(IsolationLevel.Serializable);

                //----

                string sql3 = @"select StartVersionID = CHANGE_TRACKING_CURRENT_VERSION()";
                SqlCommand sqlCmd3 = new SqlCommand(sql3, srcConn, fatUglyTransaction);
                vStartVersionID = (Int64)sqlCmd3.ExecuteScalar();

                //----

                SqlCommand cmdGetCT1 =
                    new SqlCommand(@"
                        insert into [etl].[Change_Tracking_Version] (Table_Name, Change_Tracking_Version)
                        select @srcTable, @StartVersionID
                        ", srcConn, fatUglyTransaction);
                cmdGetCT1.Parameters.AddWithValue("@StartVersionID", vStartVersionID);
                cmdGetCT1.Parameters.AddWithValue("@srcTable", srcTable);

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
                        bulkCopy.BulkCopyTimeout = 12000; // 200 minutes timeout !!!!!

                        //bulkCopy.ColumnMappings.Add("ProductID", "ProductID");
                        //bulkCopy.ColumnMappings.Add("ProductName", "Name");
                        //bulkCopy.ColumnMappings.Add("QuantityPerUnit", "Quantity");
                        bulkCopy.DestinationTableName = dstTable;
                        bulkCopy.WriteToServer(reader);
                    }
                }
                reader.Close();

                // ----
                string sqlCT_2 = @"select EndVersionID = CHANGE_TRACKING_CURRENT_VERSION()";
                SqlCommand cmdCT_2 = new SqlCommand(sqlCT_2, srcConn, fatUglyTransaction);
                Int64 vStartVersionID_2 = (Int64)cmdCT_2.ExecuteScalar();

                // Debug.Assert(vStartVersionID == vStartVersionID_2);

                // ----
                fatUglyTransaction.Commit();

            }
        }




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
                    WHERE Table_Name = @srcTable";
                SqlCommand sqlCmd2 = new SqlCommand(sql2, srcConn);
                sqlCmd2.Parameters.AddWithValue("@srcTable", srcTable);

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
                for (var c = 0; c < reader.FieldCount; c++)
                {
                    dt.Columns.Add(reader.GetName(c), reader.GetFieldType(c));
                }

                // index of last field (set to OPERATION)
                int fldOPER = reader.FieldCount - 1;

                object[] obj = new object[reader.FieldCount];

#if ACTION_ADD_ROWS
                // add rows

                while (reader.Read())
                {
                    reader.GetValues(obj);

                    //if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    //    continue;
                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    ////     continue;

                    var dr = dt.Rows.Add(obj);

                    //var idx = dt.Rows.Count;
                    //dr = dt.Rows[idx - 1];

                    Console.WriteLine(reader.GetSqlChars(fldOPER).Value[0]);

                    if (reader.GetSqlChars(fldOPER).Value[0] == 'I')
                    {
                        /* no-op */
                    }
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

                ///
                var fatUglyTransaction = dstConn.BeginTransaction(IsolationLevel.Serializable);
                ///


                var sqlCmd = new SqlCommand("select top 0 * from " + dstTable, dstConn, fatUglyTransaction);
                sqlCmd.Parameters.AddWithValue("@dstTable", dstTable);
                SqlDataAdapter da = new SqlDataAdapter(sqlCmd);
                var sqlCB = new SqlCommandBuilder(da);
                sqlCB.ConflictOption = ConflictOption.OverwriteChanges; // generate UPD/DEL commands using PK only in the WHERE clause

                //SqlCommand delCmd = new SqlCommand(@"
                //    delete from _factBookings 
                //    where 
                //            BookingID       = @BookingID
                //    and     Amendments      = @Amendments   
                //    and     BookingStatus   = @BookingStatus
                //    and     Cancellations   = @Cancellations
                //", dstConn, fatUglyTransaction);
                //
                //delCmd.Parameters.Add(new SqlParameter("@BookingID", SqlDbType.VarChar));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@BookingID"].SourceColumn = "BookingID";
                //
                //delCmd.Parameters.Add(new SqlParameter("@Amendments", SqlDbType.TinyInt));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@Amendments"].SourceColumn = "Amendments";
                //
                //delCmd.Parameters.Add(new SqlParameter("@BookingStatus", SqlDbType.TinyInt));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@BookingStatus"].SourceColumn = "BookingStatus";
                //
                //delCmd.Parameters.Add(new SqlParameter("@Cancellations", SqlDbType.TinyInt));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@Cancellations"].SourceColumn = "Cancellations";
                //
                //da.DeleteCommand = delCmd;

                da.DeleteCommand = sqlCB.GetDeleteCommand();
                da.UpdateCommand = sqlCB.GetUpdateCommand();
                da.InsertCommand = sqlCB.GetInsertCommand();

                da.DeleteCommand.Transaction = fatUglyTransaction;
                da.UpdateCommand.Transaction = fatUglyTransaction;
                da.InsertCommand.Transaction = fatUglyTransaction;

                Console.WriteLine(da.DeleteCommand.CommandText);
                Console.WriteLine(da.UpdateCommand.CommandText);
                Console.WriteLine(da.InsertCommand.CommandText);

                //var delRow = dt.Select(null, null, DataViewRowState.Deleted).FirstOrDefault();
                //DataRow[] delRows = new DataRow[1];
                //delRows[0] = delRow;
                //da.Update(delRows);

                //var delRows = dt.Select(null, null, DataViewRowState.Deleted);


                int counter = 0;
                DataTable dtCp = dt.Clone();
                dtCp.Clear();



                // Stream fs = new FileStream(  "log.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite);
                // StreamWriter sw = new StreamWriter(fs);
                // 

                foreach (DataRow dr in dt.Rows)
                {
                    DataRow[] delRWs = new DataRow[1];
                    dtCp.ImportRow(dr);
                    DataRow drr = dtCp.Rows[0];

                    delRWs[0] = drr;
                    counter++;

                    try
                    {
                        da.Update(delRWs);
                    }
                    catch (DBConcurrencyException ex)
                    {
                        Console.WriteLine("count: {0}", counter);
                        Console.WriteLine(drr.RowState);
                        if (drr.RowState == DataRowState.Deleted)
                        {
                            foreach(var i in PKColOrdinals)
                                Console.WriteLine(drr[i, DataRowVersion.Original]);
                        }
                        else
                        {
                            foreach (var i in PKColOrdinals)
                                Console.WriteLine(drr[i]);
                        }
                        //foreach (var c in dt.PrimaryKey)
                        //{
                        //    if (drr.RowState == DataRowState.Deleted)
                        //    {
                        //        Console.WriteLine(drr[c.Ordinal, DataRowVersion.Original]);
                        //    }
                        //    else
                        //    {
                        //        Console.WriteLine(drr[c.Ordinal]);
                        //    }
                        //}


                        Console.WriteLine(ex.Message);
                    }
                    dtCp.Clear();

                }

                //da.Update(dt);

                fatUglyTransaction.Commit();

                reader.Close();

                dstConn.Close();

                // ----

                SqlCommand cmdSet_CT =
                    new SqlCommand(@"
                        update [etl].[Change_Tracking_Version] 
                        set Change_Tracking_Version = @EndVersionID
                        where Table_Name= @srcTable
                        ", srcConn);
                cmdSet_CT.Parameters.AddWithValue("@EndVersionID", vEndVersionID);
                cmdSet_CT.Parameters.AddWithValue("@srcTable", srcTable);
                cmdSet_CT.ExecuteNonQuery();

            }
        }

        private static SqlCommand execNonQuery(SqlConnection srcConn, string sql_1)
        {
            SqlCommand sqlCmd3 = new SqlCommand(sql_1, srcConn);
            sqlCmd3.ExecuteNonQuery();
            return sqlCmd3;
        }

        void bulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            System.Console.WriteLine("rows copied:" + e.RowsCopied);
        }


        public void testPKColumns()
        {

            SqlConnection dstConn =
                        new SqlConnection(dstConnStr);
            dstConn.Open();

            ///
            ///
            var fatUglyTransaction = dstConn.BeginTransaction(IsolationLevel.Serializable);
            ///
            var sqlCmd = new SqlCommand("select top 0 * from " + dstTable, dstConn, fatUglyTransaction); 

            SqlDataAdapter da = new SqlDataAdapter(sqlCmd);

            DataTable dt = new DataTable();
            da.Fill(dt);

            foreach (var c in dt.PrimaryKey)
            {
                Console.WriteLine(c.Ordinal);
                //if (drr.RowState == DataRowState.Deleted)
                //{
                //    Console.WriteLine(drr[c.Ordinal, DataRowVersion.Original]);
                //}
                //else
                //{
                //    Console.WriteLine(drr[c.Ordinal]);
                //}
            }



            // Stream fs = new FileStream(  "log.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            // StreamWriter sw = new StreamWriter(fs);
            // 
            

        }

    }


}
