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
            //throw new NotImplementedException();

            return new List<Select_TestParameters>()
            {
                //new Select_TestParameters()
                //{ TestProcedure = Linq2db_SelectTest, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_Linq, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_Linq, UseTransaction = true, OneByOne = true },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_FromSql, UseTransaction = true, OneByOne = false },

                new Select_TestParameters()
                { TestProcedure = Linq2db_SelectUser_FromSql, UseTransaction = true, OneByOne = true },

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

        //private List<User> Linq2db_SelectTest(DBHelper.InnerDbConnection connection, List<User> userList, Select_TestParameters parameters)
        //{
        //    using (var cmd = new NpgsqlCommand())
        //    {
        //        cmd.CommandText = "SELECT int_num FROM public.test_nums ORDER BY id DESC LIMIT 1";
        //        cmd.Connection = (NpgsqlConnection)(connection.Connection);

        //        using (var reader = cmd.ExecuteReader())
        //        {
        //            reader.Read();
        //            var val = reader.GetFieldValue<int>(0);
        //            nlog.Info("Npgsql: {0}", val);
        //            nlog.Info("int32max: {0}, value = {1}", Int32.MaxValue, val);
        //        }
        //    }
        //    return new List<User>() { new User() };
        //}

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



                //using (var db = PostgreSQLTools.CreateDataConnection(ConnectionString))
                //{

                //}
                //using (var cmd = new NpgsqlCommand())
                //{
                //    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);
                //    foreach (var user in usersList)
                //    {
                //        selectUser = new User();
                //        cmd.CommandText = String.Format(
                //        "SELECT * FROM public.user_tbl WHERE id = {0};",
                //        user.Id
                //        );

                //        using (var reader = cmd.ExecuteReader())
                //        {
                //            if (reader.Read())
                //            {
                //                selectUser.Id = reader.GetInt64(0);
                //                selectUser.Name = reader.GetString(1);
                //                selectUser.Login_count = reader.GetInt32(2);
                //            }
                //        }
                //        selectedUsersList.Add(selectUser);
                //    }
                //}

            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                //var q =
                //    from c in db.GetTable<User>()
                //    join u in usersList on c.Id == u.Id
                //    select c;
                //var q = from c in db.GetTable<User>()
                //        where c
                {
                    var usrArr = usersList.Select(u => u.Id).ToArray();



                    var q = from u in db.GetTable<User>()
                            where usrArr.Contains(u.Id)
                            select u;

                    selectedUsersList.AddRange(q);

                    var lst = db.LastQuery;
                }

                //{
                //    var usrArr = usersList.Select(u => u.Id).ToArray();

                //    var uArr = usersList.ToArray();



                //    //var q = from u in db.GetTable<User>()
                //    //        where 
                //    //        select u;

                //    selectedUsersList.AddRange(q);

                //    var lst = db.LastQuery;
                //}


                //using (var cmd = new NpgsqlCommand())
                //{
                //    StringBuilder concatCommand = new StringBuilder(
                //        "SELECT * FROM public.user_tbl WHERE id IN ( ");

                //    //StringBuilder sb = new StringBuilder();

                //    foreach (var usr in usersList)
                //    {
                //        concatCommand.Append(usr.Id.ToString()).Append(',');
                //    }

                //    concatCommand.Remove(concatCommand.Length - 1, 1);
                //    //string ids = String.Join<long>(",", usersList.Select(user => user.Id));

                //    concatCommand.Append(" );");

                //    cmd.CommandText = concatCommand.ToString();
                //    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);


                //    using (var reader = cmd.ExecuteReader())
                //    {
                //        while (reader.Read())
                //        {
                //            User selectUser;
                //            selectUser = new User();

                //            selectUser.Id = reader.GetInt64(0);
                //            selectUser.Name = reader.GetString(1);
                //            selectUser.Login_count = reader.GetInt32(2);

                //            selectedUsersList.Add(selectUser);
                //        }
                //    }

                //}
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

        private static List<User> Linq2db_SelectUser_FromSql(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        {
            List<User> selectedUsersList = new List<User>();

            if (parameters.OneByOne)
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                //var dp = new DataParameter("usId", 0, DataType.Int64);


                foreach (var user in usersList)
                {
                    //var query = db.FromSql<User>($"SELECT * FROM public.user_tbl WHERE id = {user.Id}");
                    //var query = db.FromSql<User>($"SELECT user_tbl.id, user_tbl.name, user_tbl.login_count FROM public.user_tbl WHERE user_tbl.id = {user.Id}");
                    //StringBuilder queryBuilder = new StringBuilder("SELECT user_tbl.id, user_tbl.name, user_tbl.login_count FROM public.user_tbl WHERE user_tbl.id =");
                    //queryBuilder.Append(user.Id.ToString());
                    //var query = db.FromSql<User>(queryBuilder.ToString());

                    //var query = db.Execute<List<User>>($"SELECT * FROM public.user_tbl WHERE id = {user.Id}");
                    //var query = db.Execute<User[]>($"SELECT * FROM public.user_tbl WHERE id = {user.Id}");
                    //DataParameter dp = new DataParameter();

                    StringBuilder queryBuilder = new StringBuilder("SELECT id, name, login_count FROM public.user_tbl WHERE id = ");
                    queryBuilder.Append(user.Id.ToString());
                    //var query = db.Execute<User[]>(queryBuilder.ToString());
                    //var query = db.Execute<User[]>(queryBuilder.ToString());

                    var query = db.Query<User>(queryBuilder.ToString());
                    //var query = db.Execute<User>($"SELECT * FROM public.user_tbl WHERE id = {user.Id}");


                    //                    SELECT
                    //        t1.id,
                    //        t1.name,
                    //        t1.login_count
                    //FROM
                    //        (
                    //                SELECT * FROM public.user_tbl WHERE id = 45507540
                    //        ) t1
                    //dp.Value = user.Id;
                    //var query = db.FromSql<User>($"SELECT * FROM public.user_tbl WHERE id = {new DataParameter("usId", user.Id, DataType.Int64)}");
                    //var query = db.FromSql<User>($"SELECT * FROM public.user_tbl WHERE id = {dp} LIMIT 1");
                    //StringBuilder sb = new StringBuilder("SELECT * FROM public.user_tbl WHERE id = ");
                    //sb.Append(user.Id.ToString());

                    ////var query = db.FromSql<User>(String.Format("SELECT * FROM public.user_tbl WHERE id = {0}", user.Id));
                    //var query = db.FromSql<User>(sb.ToString());

                    //var res = query.ToArray();


                    //var q =
                    //    from c in db.GetTable<User>()
                    //    where c.Id == user.Id
                    //    select c;

                    //var q = db.SelectQuery<User>(u => u.Id)

                    selectedUsersList.AddRange(query);
                    //selectedUsersList.Add(query);
                }

                Console.WriteLine("LastQuery: =============\n{0}\n========", db.LastQuery);

                //using (var db = PostgreSQLTools.CreateDataConnection(ConnectionString))
                //{

                //}
                //using (var cmd = new NpgsqlCommand())
                //{
                //    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);
                //    foreach (var user in usersList)
                //    {
                //        selectUser = new User();
                //        cmd.CommandText = String.Format(
                //        "SELECT * FROM public.user_tbl WHERE id = {0};",
                //        user.Id
                //        );

                //        using (var reader = cmd.ExecuteReader())
                //        {
                //            if (reader.Read())
                //            {
                //                selectUser.Id = reader.GetInt64(0);
                //                selectUser.Name = reader.GetString(1);
                //                selectUser.Login_count = reader.GetInt32(2);
                //            }
                //        }
                //        selectedUsersList.Add(selectUser);
                //    }
                //}

            }
            else
            {
                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

                //var q =
                //    from c in db.GetTable<User>()
                //    join u in usersList on c.Id == u.Id
                //    select c;
                //var q = from c in db.GetTable<User>()
                //        where c
                {
                    var usrArr = usersList.Select(u => u.Id).ToArray();

                    StringBuilder concatCommand = new StringBuilder(
                            "SELECT * FROM public.user_tbl WHERE id IN ( ");

                    foreach (var usr in usersList)
                    {
                        concatCommand.Append(usr.Id.ToString()).Append(',');
                    }

                    concatCommand.Remove(concatCommand.Length - 1, 1);

                    concatCommand.Append(" )");


                    //var query = db.FromSql<User>(concatCommand.ToString());
                    //var query = db.Execute<List<User>>(concatCommand.ToString());
                    //var query = db.Execute<User[]>(concatCommand.ToString());
                    var query = db.Query<User>(concatCommand.ToString());

                    //var q = from u in db.GetTable<User>()
                    //        where usrArr.Contains(u.Id)
                    //        select u;

                    selectedUsersList.AddRange(query);

                    var lst = db.LastQuery;
                }

                //{
                //    var usrArr = usersList.Select(u => u.Id).ToArray();

                //    var uArr = usersList.ToArray();



                //    //var q = from u in db.GetTable<User>()
                //    //        where 
                //    //        select u;

                //    selectedUsersList.AddRange(q);

                //    var lst = db.LastQuery;
                //}


                //using (var cmd = new NpgsqlCommand())
                //{
                //    StringBuilder concatCommand = new StringBuilder(
                //        "SELECT * FROM public.user_tbl WHERE id IN ( ");

                //    //StringBuilder sb = new StringBuilder();

                //    foreach (var usr in usersList)
                //    {
                //        concatCommand.Append(usr.Id.ToString()).Append(',');
                //    }

                //    concatCommand.Remove(concatCommand.Length - 1, 1);
                //    //string ids = String.Join<long>(",", usersList.Select(user => user.Id));

                //    concatCommand.Append(" );");

                //    cmd.CommandText = concatCommand.ToString();
                //    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);


                //    using (var reader = cmd.ExecuteReader())
                //    {
                //        while (reader.Read())
                //        {
                //            User selectUser;
                //            selectUser = new User();

                //            selectUser.Id = reader.GetInt64(0);
                //            selectUser.Name = reader.GetString(1);
                //            selectUser.Login_count = reader.GetInt32(2);

                //            selectedUsersList.Add(selectUser);
                //        }
                //    }

                //}
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



        //private static List<User> Linq2db_SelectUser_Text_WithParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
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

        //private static List<User> Linq2db_SelectUser_Function(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
        //{
        //    DbManager db = (DbManager)(innerConnection.Connection);

        //    List<User> selectedUsersList = new List<User>();

        //    if (parameters.OneByOne)
        //    {
        //        foreach (var user in usersList)
        //        {
        //            selectedUsersList.Add(
        //                db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectbyid",
        //                db.Parameter("pid", user.Id))
        //                .ExecuteObject<User>()
        //                );
        //        }
        //    }
        //    else
        //    {
        //        var namesArray = usersList.Select((User user) => user.Id).ToArray<long>();
        //        selectedUsersList = db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectarraybyid",
        //            db.Parameter("pids", namesArray))
        //            .ExecuteList<User>();
        //    }

        //    return selectedUsersList;
        //}
    }
}


// ==========================================================================================
// ==========================================================================================
// ==========================================================================================
// ==========================================================================================

//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Diagnostics;
//using System.Threading.Tasks;
//using NLog;
//using Npgsql;
//using TestSqlSpeed_Net.Helpers;
//using BLToolkit;
//using BLToolkit.Data;
//using BLToolkit.DataAccess;
//using BLToolkit.Reflection;
//using BLToolkit.Mapping;
//using LinqToDB;


//namespace TestSqlSpeed_Net.Tests.Select
//{
//    class SelectLinq2dbTest : SelectTest
//    {
//        private static Logger nlog = LogManager.GetCurrentClassLogger();

//        public sealed override DBHelper.DbProvider DbProviderType { get { return DBHelper.DbProvider.Linq2db; } }

//        protected override List<Select_TestParameters> CreateParametersVariations()
//        {
//            //throw new NotImplementedException();

//            return new List<Select_TestParameters>()
//            {
//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectTest, UseTransaction = true, OneByOne = false },

//                new Select_TestParameters()
//                { TestProcedure = Linq2db_SelectUser_PureText, UseTransaction = true, OneByOne = false },

//                new Select_TestParameters()
//                { TestProcedure = Linq2db_SelectUser_PureText, UseTransaction = true, OneByOne = true },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = false },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = true, OneByOne = true },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = false },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = true },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_PureText, UseTransaction = false, OneByOne = false },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_PureText, UseTransaction = false, OneByOne = true },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = false },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Text_WithParameters, UseTransaction = false, OneByOne = true },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = false },

//                //new Select_TestParameters()
//                //{ TestProcedure = Linq2db_SelectUser_Function, UseTransaction = true, OneByOne = true },

//            };
//        }

//        //private List<User> Linq2db_SelectTest(DBHelper.InnerDbConnection connection, List<User> userList, Select_TestParameters parameters)
//        //{
//        //    using (var cmd = new NpgsqlCommand())
//        //    {
//        //        cmd.CommandText = "SELECT int_num FROM public.test_nums ORDER BY id DESC LIMIT 1";
//        //        cmd.Connection = (NpgsqlConnection)(connection.Connection);

//        //        using (var reader = cmd.ExecuteReader())
//        //        {
//        //            reader.Read();
//        //            var val = reader.GetFieldValue<int>(0);
//        //            nlog.Info("Npgsql: {0}", val);
//        //            nlog.Info("int32max: {0}, value = {1}", Int32.MaxValue, val);
//        //        }
//        //    }
//        //    return new List<User>() { new User() };
//        //}

//        private static List<User> Linq2db_SelectUser_PureText(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
//        {
//            List<User> selectedUsersList = new List<User>();

//            if (parameters.OneByOne)
//            {
//                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

//                foreach (var user in usersList)
//                {
//                    var q =
//                        from c in db.GetTable<User>()
//                        where c.Id == user.Id
//                        select c;

//                    //var q = db.SelectQuery<User>(u => u.Id)

//                    selectedUsersList.AddRange(q);
//                }



//                //using (var db = PostgreSQLTools.CreateDataConnection(ConnectionString))
//                //{

//                //}
//                    //using (var cmd = new NpgsqlCommand())
//                    //{
//                    //    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);
//                    //    foreach (var user in usersList)
//                    //    {
//                    //        selectUser = new User();
//                    //        cmd.CommandText = String.Format(
//                    //        "SELECT * FROM public.user_tbl WHERE id = {0};",
//                    //        user.Id
//                    //        );

//                    //        using (var reader = cmd.ExecuteReader())
//                    //        {
//                    //            if (reader.Read())
//                    //            {
//                    //                selectUser.Id = reader.GetInt64(0);
//                    //                selectUser.Name = reader.GetString(1);
//                    //                selectUser.Login_count = reader.GetInt32(2);
//                    //            }
//                    //        }
//                    //        selectedUsersList.Add(selectUser);
//                    //    }
//                    //}

//                }
//            else
//            {
//                var db = (LinqToDB.Data.DataConnection)innerConnection.Connection;

//                //var q =
//                //    from c in db.GetTable<User>()
//                //    join u in usersList on c.Id == u.Id
//                //    select c;
//                //var q = from c in db.GetTable<User>()
//                //        where c
//                {
//                    var usrArr = usersList.Select(u => u.Id).ToArray();



//                    var q = from u in db.GetTable<User>()
//                            where usrArr.Contains(u.Id)
//                            select u;

//                    selectedUsersList.AddRange(q);

//                    var lst = db.LastQuery;
//                }

//                //{
//                //    var usrArr = usersList.Select(u => u.Id).ToArray();

//                //    var uArr = usersList.ToArray();



//                //    //var q = from u in db.GetTable<User>()
//                //    //        where 
//                //    //        select u;

//                //    selectedUsersList.AddRange(q);

//                //    var lst = db.LastQuery;
//                //}


//                //using (var cmd = new NpgsqlCommand())
//                //{
//                //    StringBuilder concatCommand = new StringBuilder(
//                //        "SELECT * FROM public.user_tbl WHERE id IN ( ");

//                //    //StringBuilder sb = new StringBuilder();

//                //    foreach (var usr in usersList)
//                //    {
//                //        concatCommand.Append(usr.Id.ToString()).Append(',');
//                //    }

//                //    concatCommand.Remove(concatCommand.Length - 1, 1);
//                //    //string ids = String.Join<long>(",", usersList.Select(user => user.Id));

//                //    concatCommand.Append(" );");

//                //    cmd.CommandText = concatCommand.ToString();
//                //    cmd.Connection = (NpgsqlConnection)(innerConnection.Connection);


//                //    using (var reader = cmd.ExecuteReader())
//                //    {
//                //        while (reader.Read())
//                //        {
//                //            User selectUser;
//                //            selectUser = new User();

//                //            selectUser.Id = reader.GetInt64(0);
//                //            selectUser.Name = reader.GetString(1);
//                //            selectUser.Login_count = reader.GetInt32(2);

//                //            selectedUsersList.Add(selectUser);
//                //        }
//                //    }

//                //}
//            }


//            //DbManager db = (DbManager)(innerConnection.Connection);

//            //List<User> selectedUsersList = new List<User>();

//            //if (parameters.OneByOne)
//            //{
//            //    User selectedUser;

//            //    foreach (var user in usersList)
//            //    {
//            //        selectedUser = db.SetCommand(
//            //        (new StringBuilder()).AppendFormat(
//            //            "SELECT * FROM public.user_tbl WHERE id = {0};",
//            //            user.Id
//            //            ).ToString())
//            //            .ExecuteObject<User>();
//            //        if (selectedUser != null)
//            //        {
//            //            selectedUsersList.Add(selectedUser);
//            //        }
//            //    }

//            //}
//            //else
//            //{
//            //    StringBuilder concatCommand = new StringBuilder(
//            //        "SELECT * FROM public.user_tbl WHERE id IN ");

//            //    StringBuilder sb = new StringBuilder();

//            //    string ids = String.Join<long>(",", usersList.Select(user => user.Id));

//            //    concatCommand.AppendFormat("( {0} );", ids);

//            //    db.SetCommand(concatCommand.ToString());

//            //    selectedUsersList = db.ExecuteList<User>();

//            //}

//            return selectedUsersList;
//        }



//        //private static List<User> Linq2db_SelectUser_Text_WithParameters(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
//        //{
//        //    DbManager db = (DbManager)(innerConnection.Connection);

//        //    List<User> selectedUsersList = new List<User>();

//        //    if (parameters.OneByOne)
//        //    {
//        //        foreach (var user in usersList)
//        //        {
//        //            selectedUsersList.Add(
//        //                db.SetCommand("SELECT * FROM public.user_tbl WHERE id = @i;",
//        //                db.Parameter("@i", user.Id))
//        //                .ExecuteObject<User>()
//        //                );
//        //        }
//        //    }
//        //    else
//        //    {
//        //        StringBuilder concatCommand = new StringBuilder(
//        //            "SELECT * FROM public.user_tbl WHERE id = ANY(SELECT unnest(array[@i]));");

//        //        var id_array = usersList.Select((User user) => user.Id).ToArray<long>();

//        //        selectedUsersList = db.SetCommand(concatCommand.ToString(),
//        //            db.Parameter("@i", id_array))
//        //            .ExecuteList<User>();
//        //    }

//        //    return selectedUsersList;
//        //}

//        private static List<User> Linq2db_SelectUser_Function(DBHelper.InnerDbConnection innerConnection, List<User> usersList, Select_TestParameters parameters)
//        {
//            DbManager db = (DbManager)(innerConnection.Connection);

//            List<User> selectedUsersList = new List<User>();

//            if (parameters.OneByOne)
//            {
//                foreach (var user in usersList)
//                {
//                    selectedUsersList.Add(
//                        db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectbyid",
//                        db.Parameter("pid", user.Id))
//                        .ExecuteObject<User>()
//                        );
//                }
//            }
//            else
//            {
//                var namesArray = usersList.Select((User user) => user.Id).ToArray<long>();
//                selectedUsersList = db.SetCommand(System.Data.CommandType.StoredProcedure, "user_selectarraybyid",
//                    db.Parameter("pids", namesArray))
//                    .ExecuteList<User>();
//            }

//            return selectedUsersList;
//        }
//    }
//}
