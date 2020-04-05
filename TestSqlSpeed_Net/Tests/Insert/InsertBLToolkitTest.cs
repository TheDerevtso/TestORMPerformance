using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading.Tasks;
using NLog;
using Npgsql;
using TestSqlSpeed_Net.Helpers;
using BLToolkit;
using BLToolkit.Data;
using BLToolkit.DataAccess;
using BLToolkit.Reflection;
using BLToolkit.Mapping;


namespace TestSqlSpeed_Net.Tests.Insert
{
    class InsertBLToolkitTest : InsertTest
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public sealed override DBHelper.DbProvider DbProviderType { get { return DBHelper.DbProvider.BLToolkit; } }

        protected override List<Insert_TestParameters> CreateParametersVariations()
        {
            return new List<Insert_TestParameters>()
                {
                    //new Insert_TestParameters()
                    //{ TestProcedure = BLToolkit_InsertUser_PureText, UseTransaction = true, OneByOne = true },

                    //new Insert_TestParameters()
                    //{ TestProcedure = BLToolkit_InsertUser_PureText, UseTransaction = true, OneByOne = false },

                    //new Insert_TestParameters()
                    //{ TestProcedure = BLToolkit_InsertUser_Text_WithParameters, UseTransaction = true, OneByOne = true },

                    //new Insert_TestParameters()
                    //{ TestProcedure = BLToolkit_InsertUser_Text_WithParameters, UseTransaction = true, OneByOne = false },

                    /*
                     * disabled fot do not messing with Prepare
                     * new TestInsertParameters()
                     * { TestProcedure = BLToolkit_InsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = true }, 
                     * 
                     * new TestInsertParameters()
                     * { TestProcedure = BLToolkit_InsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = false },
                     * 
                     * new TestInsertParameters()
                     * { TestProcedure = BLToolkit_InsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = true },
                     * 
                     * new TestInsertParameters()
                     * { TestProcedure = BLToolkit_InsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = false },
                    */

                    new Insert_TestParameters()
                    { TestProcedure = BLToolkit_InsertUser_Function, UseTransaction = true, OneByOne = true },

                    //new Insert_TestParameters()
                    //{ TestProcedure = BLToolkit_InsertUser_Function, UseTransaction = true, OneByOne = false },

                    //disabled variations without transactions because of slowness
                     //new Insert_TestParameters()
                     //{ TestProcedure = BLToolkit_InsertUser_PureText, UseTransaction = false, OneByOne = true },
                     
                     //new Insert_TestParameters()
                     //{ TestProcedure = BLToolkit_InsertUser_Text_WithParameters, UseTransaction = false, OneByOne = true },
                     
                     //new Insert_TestParameters()
                     //{ TestProcedure = BLToolkit_InsertUser_Function, UseTransaction = false, OneByOne = true },
                     
                     //new Insert_TestParameters()
                     //{ TestProcedure = BLToolkit_InsertUser_Function, UseTransaction = false, OneByOne = false },

                };
        }



        private static void BLToolkit_InsertUser_PureText(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            DbManager db = (DbManager)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    db.SetCommand(
                    (new StringBuilder()).AppendFormat(
                        "INSERT INTO public.user_tbl(name, login_count) VALUES ('{0}', {1})",
                        user.Name, user.Login_count
                        ).ToString())
                        .ExecuteNonQuery();
                }
            }
            else
            {
                StringBuilder concatCommand = new StringBuilder(
                    "INSERT INTO public.user_tbl(name, login_count) SELECT ");

                string names = String.Join<string>("','", usersList.Select((User user) => user.Name));
                string logins = String.Join<int>(",", usersList.Select((User user) => user.Login_count));


                concatCommand.AppendFormat("unnest( array['{0}'] ), unnest(array[ {1} ] );", names, logins);

                db.SetCommand(concatCommand.ToString()).ExecuteNonQuery();

            }
        }

        private static void BLToolkit_InsertUser_Text_WithParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            DbManager db = (DbManager)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    db.SetCommand("INSERT INTO public.user_tbl(name, login_count) VALUES (@n, @c)",
                        db.Parameter("@n", user.Name),
                        db.Parameter("@c", user.Login_count))
                        .ExecuteNonQuery();
                }
            }
            else
            {
                StringBuilder concatCommand = new StringBuilder(
                    "INSERT INTO public.user_tbl(name, login_count) SELECT ");

                var namesArray = usersList.Select((User user) => user.Name).ToArray<string>();
                var login_countArray = usersList.Select((User user) => user.Login_count).ToArray<int>();

                concatCommand.Append("unnest( array[@n] ), unnest(array[@c] );");

                db.SetCommand(concatCommand.ToString(),
                    db.Parameter("@n", namesArray),
                    db.Parameter("@c", login_countArray))
                    .ExecuteNonQuery();
            }
        }

        //private static void BLToolkit_InsertUser_Text_WithPreparedParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        //{
        //    NpgsqlConnection conn = (NpgsqlConnection)(innerConnection.Connection);

        //    if (parameters.OneByOne)
        //    {
        //        foreach (var user in usersList)
        //        {
        //            using (NpgsqlCommand insertComm = new NpgsqlCommand(
        //            "INSERT INTO public.user_tbl(name, login_count) VALUES (@n, @c)", conn)
        //            )
        //            {
        //                var str = new NpgsqlParameter("n", NpgsqlTypes.NpgsqlDbType.Varchar);
        //                var c = new NpgsqlParameter("c", NpgsqlTypes.NpgsqlDbType.Integer);

        //                insertComm.Parameters.Add(str);
        //                insertComm.Parameters.Add(c);

        //                insertComm.Prepare();

        //                insertComm.Parameters[0].Value = user.Name;
        //                insertComm.Parameters[1].Value = user.Login_count;

        //                insertComm.ExecuteNonQuery();
        //            }
        //        }
        //    }
        //    else
        //    {
        //        using (NpgsqlCommand insertComm = new NpgsqlCommand("INSERT INTO public.user_tbl(name, login_count) SELECT unnest( array[@n] ), unnest(array[@c] );", conn))
        //        {
        //            var namesArray = usersList.Select((User user) => user.Name).ToArray<string>();
        //            var login_countArray = usersList.Select((User user) => user.Login_count).ToArray<int>();

        //            var strParam = new NpgsqlParameter("@n", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Varchar);
        //            var cParam = new NpgsqlParameter("@c", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer);

        //            insertComm.Parameters.Add(strParam);
        //            insertComm.Parameters.Add(cParam);

        //            insertComm.Prepare();

        //            insertComm.Parameters[0].Value = namesArray;
        //            insertComm.Parameters[1].Value = login_countArray;

        //            insertComm.ExecuteNonQuery();
        //        }
        //    }
        //}

        private static void BLToolkit_InsertUser_Function(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            DbManager db = (DbManager)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    string name = user.Name;
                    int login_count = user.Login_count;

                    db.SetCommand(System.Data.CommandType.StoredProcedure, "user_insert",
                        db.Parameter("pname", name),
                        db.Parameter("plogin_count", login_count))
                        .ExecuteNonQuery();
                }
            }
            else
            { 
                var namesArray = usersList.Select((User user) => user.Name).ToArray<string>();
                var login_countArray = usersList.Select((User user) => user.Login_count).ToArray<int>();

                //db.SetCommand(System.Data.CommandType.StoredProcedure, "user_insert_array",
                //    db.Parameter("pnameArr", namesArray),
                //    db.Parameter("plogin_countArr", login_countArray))
                //    .ExecuteNonQuery();


                //db.SetCommand(System.Data.CommandType.StoredProcedure, "user_insert_array");

                var par1 = db.InputParameter("pnameArr", namesArray);



                var par2 = db.InputParameter("plogin_countArr", login_countArray);

                System.Data.IDbDataParameter[] parArr = new System.Data.IDbDataParameter[] { par1, par2 };

                db.SetCommand(System.Data.CommandType.StoredProcedure, "user_insert_array", parArr);

                db.ExecuteNonQuery();
            }
        }
    }
}
