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
    class SelectBLToolkitTest : SelectTest
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public sealed override DBHelper.DbProvider DbProviderType { get { return DBHelper.DbProvider.BLToolkit; } }

        protected override List<Select_TestParameters> CreateParametersVariations()
        {
            return new List<Select_TestParameters>()
            {
                new Select_TestParameters()
                { TestProcedure = BLToolkit_SelectUser_PureText, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = BLToolkit_SelectUser_PureText, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Function, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Function, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_PureText, UseTransaction = false, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_PureText, UseTransaction = false, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Function, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = BLToolkit_SelectUser_Function, UseTransaction = true, OneByOne = true },

            };
        }

        private static List<User> BLToolkit_SelectUser_PureText(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            DbManager db = (DbManager)(innerConnection.Connection);

            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                User selectedUser;

                foreach (var user in usersList)
                {
                    selectedUser = db.SetCommand(
                    (new StringBuilder()).AppendFormat(
                        "SELECT * FROM public.user_tbl WHERE id = {0};",
                        user.Id
                        ).ToString())
                        .ExecuteObject<User>();
                    if (selectedUser != null)
                    {
                        selectedUsersList.Add(selectedUser);
                    }
                }

            }
            else
            {
                StringBuilder concatCommand = new StringBuilder(
                    "SELECT * FROM public.user_tbl WHERE id IN ");

                StringBuilder sb = new StringBuilder();

                string ids = String.Join<long>(",", usersList.Select(user => user.Id));

                concatCommand.AppendFormat("( {0} );", ids);

                db.SetCommand(concatCommand.ToString());

                selectedUsersList = db.ExecuteList<User>();

            }

            return selectedUsersList;
        }



        private static List<User> BLToolkit_SelectUser_Text_WithParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            DbManager db = (DbManager)(innerConnection.Connection);

            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                foreach (var user in usersList)
                {
                    selectedUsersList.Add(
                        db.SetCommand("SELECT * FROM public.user_tbl WHERE id = @i;",
                        db.Parameter("@i", user.Id))
                        .ExecuteObject<User>()
                        );
                }
            }
            else
            {
                StringBuilder concatCommand = new StringBuilder(
                    "SELECT * FROM public.user_tbl WHERE id = ANY(SELECT unnest(array[@i]));");

                var id_array = usersList.Select((User user) => user.Id).ToArray<long>();

                selectedUsersList = db.SetCommand(concatCommand.ToString(),
                    db.Parameter("@i", id_array))
                    .ExecuteList<User>();
            }

            return selectedUsersList;
        }

        private static List<User> BLToolkit_SelectUser_Function(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
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

                Array arar = namesArray;

                selectedUsersList = db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectarraybyid",
                    db.Parameter("pids", arar))
                    .ExecuteList<User>();
            }

            return selectedUsersList;
        }

        private static List<User> BLToolkit_SelectUser_ORM(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
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
