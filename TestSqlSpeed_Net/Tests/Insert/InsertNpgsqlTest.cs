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
    class InsertNpgsqlTest : InsertTest
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public sealed override DBHelper.DbProvider DbProviderType { get { return DBHelper.DbProvider.Npgsql;  } }

        protected override List<Insert_TestParameters> CreateParametersVariations()
        {
            return new List<Insert_TestParameters>()
                {
                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_PureText, UseTransaction = true, OneByOne = true },

                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_PureText, UseTransaction = true, OneByOne = false },

                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_Text_WithParameters, UseTransaction = true, OneByOne = true },

                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_Text_WithParameters, UseTransaction = true, OneByOne = false },
                    /*
                     * disabled fot do not messing with Prepare
                     * new TestInsertParameters()
                     * { TestProcedure = NpgsqlInsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = true }, 
                     * 
                     * new TestInsertParameters()
                     * { TestProcedure = NpgsqlInsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = false },
                     * 
                     * new TestInsertParameters()
                     * { TestProcedure = NpgsqlInsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = true },
                     * 
                     * new TestInsertParameters()
                     * { TestProcedure = NpgsqlInsertUser_Text_WithPreparedParameters, UseTransaction = true, OneByOne = false },
                    */

                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_Function, UseTransaction = true, OneByOne = true },

                    new Insert_TestParameters()
                    { TestProcedure = NpgsqlInsertUser_Function, UseTransaction = true, OneByOne = false },

                    //disabled variations without transactions because of slowness
                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_PureText, UseTransaction = false, OneByOne = true },
                    
                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_Text_WithParameters, UseTransaction = false, OneByOne = true },
                    
                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_Function, UseTransaction = false, OneByOne = true },
                    
                    //new Insert_TestParameters()
                    //{ TestProcedure = NpgsqlInsertUser_Function, UseTransaction = false, OneByOne = false },
                    
                };
        }

        

        private static void NpgsqlInsertUser_PureText(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            NpgsqlConnection conn = (NpgsqlConnection)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    using (NpgsqlCommand insertComm = new NpgsqlCommand(
                    (new StringBuilder()).AppendFormat(
                        "INSERT INTO public.user_tbl(name, login_count) VALUES ('{0}', {1})",
                        user.Name, user.Login_count
                        ).ToString(), conn)
                    )
                    {
                        insertComm.ExecuteNonQuery();
                    };
                }
            }
            else
            {
                StringBuilder concatCommand = new StringBuilder(
                    "INSERT INTO public.user_tbl(name, login_count) SELECT ");

                string names = String.Join<string>("','", usersList.Select((User user) => user.Name));
                string logins = String.Join<int>(",", usersList.Select((User user) => user.Login_count));


                concatCommand.AppendFormat("unnest( array['{0}'] ), unnest(array[ {1} ] );", names, logins);

                using (NpgsqlCommand insertComm = new NpgsqlCommand(concatCommand.ToString(), conn))
                {
                    insertComm.ExecuteNonQuery();
                };
            }
        }

        private static void NpgsqlInsertUser_Text_WithParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            NpgsqlConnection conn = (NpgsqlConnection)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    using (NpgsqlCommand insertComm = new NpgsqlCommand(
                    "INSERT INTO public.user_tbl(name, login_count) VALUES (@n, @c)", conn)
                    )
                    {
                        insertComm.Parameters.AddWithValue("n", user.Name);
                        insertComm.Parameters.AddWithValue("c", user.Login_count);

                        insertComm.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                StringBuilder concatCommand = new StringBuilder(
                    "INSERT INTO public.user_tbl(name, login_count) SELECT ");

                var namesArray = usersList.Select((User user) => user.Name).ToArray<string>();
                var login_countArray = usersList.Select((User user) => user.Login_count).ToArray<int>();

                concatCommand.Append("unnest( array[@n] ), unnest(array[@c] );");

                using (NpgsqlCommand insertComm = new NpgsqlCommand(concatCommand.ToString(), conn))
                {
                    insertComm.Parameters.AddWithValue("n", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Varchar, namesArray);
                    insertComm.Parameters.AddWithValue("c", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, login_countArray);

                    insertComm.ExecuteNonQuery();
                };
            }
        }

        private static void NpgsqlInsertUser_Text_WithPreparedParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            NpgsqlConnection conn = (NpgsqlConnection)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    using (NpgsqlCommand insertComm = new NpgsqlCommand(
                    "INSERT INTO public.user_tbl(name, login_count) VALUES (@n, @c)", conn)
                    )
                    {
                        var str = new NpgsqlParameter("n", NpgsqlTypes.NpgsqlDbType.Varchar);
                        var c = new NpgsqlParameter("c", NpgsqlTypes.NpgsqlDbType.Integer);

                        insertComm.Parameters.Add(str);
                        insertComm.Parameters.Add(c);

                        insertComm.Prepare();

                        insertComm.Parameters[0].Value = user.Name;
                        insertComm.Parameters[1].Value = user.Login_count;

                        insertComm.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                using (NpgsqlCommand insertComm = new NpgsqlCommand("INSERT INTO public.user_tbl(name, login_count) SELECT unnest( array[@n] ), unnest(array[@c] );", conn))
                {
                    var namesArray = usersList.Select((User user) => user.Name).ToArray<string>();
                    var login_countArray = usersList.Select((User user) => user.Login_count).ToArray<int>();

                    var strParam = new NpgsqlParameter("@n", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Varchar);
                    var cParam = new NpgsqlParameter("@c", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer);

                    insertComm.Parameters.Add(strParam);
                    insertComm.Parameters.Add(cParam);

                    insertComm.Prepare();

                    insertComm.Parameters[0].Value = namesArray;
                    insertComm.Parameters[1].Value = login_countArray;

                    insertComm.ExecuteNonQuery();
                }
            }
        }

        private static void NpgsqlInsertUser_Function(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Insert_TestParameters parameters)
        {
            NpgsqlConnection conn = (NpgsqlConnection)(innerConnection.Connection);

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    using (NpgsqlCommand insertComm = new NpgsqlCommand("user_insert", conn))
                    {
                        insertComm.CommandType = System.Data.CommandType.StoredProcedure;
                        insertComm.Parameters.AddWithValue("pname", user.Name);
                        insertComm.Parameters.AddWithValue("plogin_count", user.Login_count);

                        insertComm.ExecuteNonQuery();
                    }
                }
            }
            else
            {

                var namesArray = usersList.Select((User user) => user.Name).ToArray<string>();
                var login_countArray = usersList.Select((User user) => user.Login_count).ToArray<int>();

                using (NpgsqlCommand insertComm = new NpgsqlCommand("user_insert_array", conn))
                {
                    insertComm.CommandType = System.Data.CommandType.StoredProcedure;
                    insertComm.Parameters.AddWithValue("pnameArr", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Varchar, namesArray);
                    insertComm.Parameters.AddWithValue("plogin_countArr", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, login_countArray);

                    insertComm.ExecuteNonQuery();
                };
            }
        }
    }
}
