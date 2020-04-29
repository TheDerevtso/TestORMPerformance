using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading.Tasks;
using NLog;
using Npgsql;
using TestSqlSpeed_Net.Helpers;
//using BLToolkit;
//using BLToolkit.Data;
//using BLToolkit.DataAccess;
//using BLToolkit.Reflection;
//using BLToolkit.Mapping;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Linq.Builder;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;


namespace TestSqlSpeed_Net.Tests.Select
{
    class SelectLinq2dbTest : SelectTest
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public sealed override DBHelper.DbProvider DbProviderType { get { return DBHelper.DbProvider.Linq2db; } }

        protected override List<Select_TestParameters> CreateParametersVariations()
        {

            return new List<Select_TestParameters>()
            {
                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectTest, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_Linq, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_Linq, UseTransaction = true, OneByOne = true },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_LinqCompiled, UseTransaction = true, OneByOne = true },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_Query, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_Query, UseTransaction = true, OneByOne = true },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_FromSql, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_FromSql, UseTransaction = true, OneByOne = true },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_FromSql_Params, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_FromSql_Params, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_PureText, UseTransaction = false, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_PureText, UseTransaction = false, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = true },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = false },

                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = true },

            };
        }

        private static List<User> Linq2db_SelectUser_Linq(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                foreach (var user in usersList)
                {
                    //var query = db.FromSql<User>($"SELECT * FROM public.user_tbl WHERE id = {user.Id};");


                    var q =
                        from c in db.GetTable<User>()
                        where c.Id == user.Id
                        select c;

                    //var q = db.SelectQuery<User>(u => u.Id)

                    selectedUsersList.AddRange(q);
                }


            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                {
                    var usrArr = usersList.Select(u => u.Id).ToArray();

                    var q = from u in db.GetTable<User>()
                            where usrArr.Contains(u.Id)
                            select u;

                    selectedUsersList.AddRange(q);

                    //var lst = db.LastQuery;
                }

            }

            return selectedUsersList;
        }

        private static List<User> Linq2db_SelectUser_LinqCompiled(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                var _compiled = CompiledQuery.Compile<DataConnection, long, IQueryable<User>>(
                    (_db, userId) => from c in _db.GetTable<User>()
                     where c.Id == userId
                     select c);

                foreach (var user in usersList)
                {
                    //var query = db.FromSql<User>($"SELECT * FROM public.user_tbl WHERE id = {user.Id};");

                    selectedUsersList.AddRange(_compiled(db, user.Id));
                }

            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                {
                    var usrArr = usersList.Select(u => u.Id).ToArray();

                    var q = from u in db.GetTable<User>()
                            where usrArr.Contains(u.Id)
                            select u;

                    selectedUsersList.AddRange(q);
                }
            }


            return selectedUsersList;
        }

        private static List<User> Linq2db_SelectUser_Query(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                //var dp = new DataParameter("usId", 0, DataType.Int64);

                foreach (var user in usersList)
                {
                    StringBuilder queryBuilder = new StringBuilder("SELECT id, name, login_count FROM public.user_tbl WHERE id = ");
                    queryBuilder.Append(user.Id.ToString());

                    var query = db.Query<User>(queryBuilder.ToString());


                    foreach (var record in query)
                    {
                        selectedUsersList.Add(record);
                    }
                    //selectedUsersList.AddRange(query.ToArray());
                    //selectedUsersList.Add(query);
                }
            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                {
                    //var usrArr = usersList.Select(u => u.Id).ToArray();

                    StringBuilder concatCommand = new StringBuilder(
                            "SELECT * FROM public.user_tbl WHERE id IN ( ");

                    foreach (var usr in usersList)
                    {
                        concatCommand.Append(usr.Id.ToString()).Append(',');
                    }

                    concatCommand.Remove(concatCommand.Length - 1, 1);

                    concatCommand.Append(" )");

                    var query = db.Query<User>(concatCommand.ToString());


                    selectedUsersList.AddRange(query);

                }
            }

            return selectedUsersList;
        }

        private static List<User> Linq2db_SelectUser_FromSql(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                foreach (var user in usersList)
                {

                    StringBuilder queryBuilder = new StringBuilder("SELECT id, name, login_count FROM public.user_tbl WHERE id = ");
                    queryBuilder.Append(user.Id.ToString());
                    
                    var query = db.FromSql<User>(queryBuilder.ToString());
                    
                    selectedUsersList.AddRange(query);
                }


            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                {
                    //var usrArr = usersList.Select(u => u.Id).ToArray();

                    StringBuilder concatCommand = new StringBuilder(
                            "SELECT * FROM public.user_tbl WHERE id IN ( ");

                    foreach (var usr in usersList)
                    {
                        concatCommand.Append(usr.Id.ToString()).Append(',');
                    }

                    concatCommand.Remove(concatCommand.Length - 1, 1);

                    concatCommand.Append(" )");

                    var query = db.FromSql<User>(concatCommand.ToString());

                    selectedUsersList.AddRange(query);
                }

            }

            return selectedUsersList;
        }

        private static List<User> Linq2db_SelectUser_FromSql_Params(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                foreach (var user in usersList)
                {

                    var query = db.FromSql<User>("SELECT * FROM public.user_tbl WHERE id = {0}", user.Id);

                    selectedUsersList.AddRange(query);

                }
            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                {
                    //var usrArr = usersList.Select(u => u.Id).ToArray();

                    StringBuilder concatCommand = new StringBuilder(
                            "SELECT * FROM public.user_tbl WHERE id IN ( ");

                    foreach (var usr in usersList)
                    {
                        concatCommand.Append(usr.Id.ToString()).Append(',');
                    }

                    concatCommand.Remove(concatCommand.Length - 1, 1);

                    concatCommand.Append(" )");


                    var query = db.FromSql<User>(concatCommand.ToString());

                    selectedUsersList.AddRange(query);

                }

            }

            return selectedUsersList;
        }
    }
}


