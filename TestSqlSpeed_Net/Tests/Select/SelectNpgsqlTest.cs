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


namespace TestSqlSpeed_Net.Tests.Select
{
    class SelectNpgsqlTest : SelectTest
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public sealed override DBHelper.DbProvider DbProviderType { get { return DBHelper.DbProvider.Npgsql; } }

        protected override List<Select_TestParameters> CreateParametersVariations()
        {
            //throw new NotImplementedException();

            return new List<Select_TestParameters>()
            {
                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectTest, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Npgsql_SelectUser_PureText, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Npgsql_SelectUser_PureText, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Function, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Function, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_PureText, UseTransaction = false, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_PureText, UseTransaction = false, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Function, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Npgsql_SelectUser_Function, UseTransaction = true, OneByOne = true },

            };
        }

        private List<User> Npgsql_SelectTest(DBHelper.InnerDbConnection connection, List<User> userList, Select_TestParameters parameters)
        {
            using (var cmd = new NpgsqlCommand())
            {
                cmd.CommandText = "SELECT int_num FROM public.test_nums ORDER BY id DESC LIMIT 1";
                cmd.Connection = (NpgsqlConnection)(connection.Connection);

                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    var val = reader.GetFieldValue<int>(0);
                    nlog.Info("Npgsql: {0}", val);
                    nlog.Info("int32max: {0}, value = {1}", Int32.MaxValue, val);
                }
            }
            return new List<User>() { new User() };
        }

        private static List<User> Npgsql_SelectUser_PureText(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                User selectUser;


                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);
                    foreach (var user in usersList)
                    {
                        selectUser = new User();
                        cmd.CommandText = String.Format(
                        "SELECT * FROM public.user_tbl WHERE id = {0};",
                        user.Id
                        );

                        using (var reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                selectUser.Id = reader.GetInt64(0);
                                selectUser.Name = reader.GetString(1);
                                selectUser.Login_count = reader.GetInt32(2);
                            }
                        }
                        selectedUsersList.Add(selectUser);
                    }
                }

            }
            else
            {
                using (var cmd = new NpgsqlCommand())
                {
                    StringBuilder concatCommand = new StringBuilder(
                        "SELECT * FROM public.user_tbl WHERE id IN ( ");

                    //StringBuilder sb = new StringBuilder();

                    foreach (var usr in usersList)
                    {
                        concatCommand.Append(usr.Id.ToString()).Append(',');
                    }

                    concatCommand.Remove(concatCommand.Length - 1, 1);
                    //string ids = String.Join<long>(",", usersList.Select(user => user.Id));

                    concatCommand.Append(" );");

                    cmd.CommandText = concatCommand.ToString();
                    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);


                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            User selectUser;
                            selectUser = new User();

                            selectUser.Id = reader.GetInt64(0);
                            selectUser.Name = reader.GetString(1);
                            selectUser.Login_count = reader.GetInt32(2);

                            selectedUsersList.Add(selectUser);
                        }
                    }

                }
            }


            //DbManager db = (DbManager)(innerConnection.Connection);

            //List<User> selectedUsersList = new List<User>();

            //if (parameters.OneByOne)
            //{
            //    User selectedUser;

            //    foreach (var user in usersList)
            //    {
            //        selectedUser = db.SetCommand(
            //        (new StringBuilder()).AppendFormat(
            //            "SELECT * FROM public.user_tbl WHERE id = {0};",
            //            user.Id
            //            ).ToString())
            //            .ExecuteObject<User>();
            //        if (selectedUser != null)
            //        {
            //            selectedUsersList.Add(selectedUser);
            //        }
            //    }

            //}
            //else
            //{
            //    StringBuilder concatCommand = new StringBuilder(
            //        "SELECT * FROM public.user_tbl WHERE id IN ");

            //    StringBuilder sb = new StringBuilder();

            //    string ids = String.Join<long>(",", usersList.Select(user => user.Id));

            //    concatCommand.AppendFormat("( {0} );", ids);

            //    db.SetCommand(concatCommand.ToString());

            //    selectedUsersList = db.ExecuteList<User>();

            //}

            return selectedUsersList;
        }



        //private static List<User> Npgsql_SelectUser_Text_WithParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        //{
        //    DbManager db = (DbManager)(innerConnection.Connection);

        //    List<User> selectedUsersList = new List<User>();

        //    if (parameters.OneByOne)
        //    {
        //        foreach (var user in usersList)
        //        {
        //            selectedUsersList.Add(
        //                db.SetCommand("SELECT * FROM public.user_tbl WHERE id = @i;",
        //                db.Parameter("@i", user.Id))
        //                .ExecuteObject<User>()
        //                );
        //        }
        //    }
        //    else
        //    {
        //        StringBuilder concatCommand = new StringBuilder(
        //            "SELECT * FROM public.user_tbl WHERE id = ANY(SELECT unnest(array[@i]));");

        //        var id_array = usersList.Select((User user) => user.Id).ToArray<long>();

        //        selectedUsersList = db.SetCommand(concatCommand.ToString(),
        //            db.Parameter("@i", id_array))
        //            .ExecuteList<User>();
        //    }

        //    return selectedUsersList;
        //}

        private static List<User> Npgsql_SelectUser_Function(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            DbManager db = (DbManager)(innerConnection.Connection);

            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    selectedUsersList.Add(
                        db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectbyid",
                        db.Parameter("pid", user.Id))
                        .ExecuteObject<User>()
                        );
                }
            }
            else
            {
                var namesArray = usersList.Select((User user) => user.Id).ToArray<long>();
                selectedUsersList = db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectarraybyid",
                    db.Parameter("pids", namesArray))
                    .ExecuteList<User>();
            }

            return selectedUsersList;
        }
    }
}
