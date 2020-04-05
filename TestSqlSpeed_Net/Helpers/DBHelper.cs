using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;
using Npgsql;
using BLToolkit;
using BLToolkit.Data;
using BLToolkit.DataAccess;
using BLToolkit.Mapping;
using BLToolkit.Reflection;
using LinqToDB;
using LinqToDB.DataProvider.PostgreSQL;
//using LinqToDB.Configuration;
using System.Configuration;

namespace TestSqlSpeed_Net.Helpers
{
    public static class DBHelper
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public class InnerDbCredentials
        {
            public string Host = "127.0.0.1";
            public int Port = 5432;
            public string UserName = "username_mock";
            public string Password = "pass_mock";
            public string Database = "dbname_mock";

        }

        private static DbProvider[] usingDBProviders = new DbProvider[]
        {
            DbProvider.Npgsql,
            DbProvider.BLToolkit,
            DbProvider.Linq2db
        };

        private static InnerDbCredentials currentCredentials = null;

        public static InnerDbCredentials CurrentCredentials
        {
            get
            {
                if (currentCredentials == null)
                {
                    currentCredentials = new InnerDbCredentials();
                }

                return currentCredentials;
            }
            set
            {
                currentCredentials = value;

                RenewCachedConnectionStrings();
            }
        }

        private static List<string> connectionStringCache = new List<string>();

        private static void RenewCachedConnectionStrings()
        {
            connectionStringCache.Clear();

            foreach (var provider in usingDBProviders)
            {
                connectionStringCache.Add(GenerateConnectionString(provider));
            }
        }

        public static bool ValidateCredentials()
        {
            try
            {
                using (DBHelper.InnerDbConnection conn = new DBHelper.InnerDbConnection(DBHelper.DbProvider.Npgsql))
                {
                    conn.Open();
                }

                return true;
            }
            catch (Exception ex)
            {
                nlog.Error(ex, "Invalid db credentials: {0}", GetConnectionString(DbProvider.Npgsql));
            }

            return false;
        }

        public sealed class InnerDbConnection : IDisposable
        {
            private DbProvider dbProvider;
            private object connection;

            private static bool isBltoolkitPrepared = false;

            internal DbProvider DbProvider { get => dbProvider; private set => dbProvider = value; }
            public object Connection { get => connection; private set => connection = value; }

            public InnerDbConnection(DbProvider dbProvider)
            {
                switch (dbProvider)
                {
                    case DbProvider.Npgsql:
                        Connection = new NpgsqlConnection(GetConnectionString(DbProvider.Npgsql));
                        break;
                    case DbProvider.BLToolkit:
                        Connection = GetBLToolkitDbManager();
                        break;
                    case DbProvider.Linq2db:
                        Connection = PostgreSQLTools.CreateDataConnection(GetConnectionString(DbProvider.Linq2db));
                        break;
                    default:
                        break;
                }

                DbProvider = dbProvider;
            }

            public void Open()
            {
                switch (DbProvider)
                {
                    case DbProvider.Npgsql:
                        ((NpgsqlConnection)connection).Open();
                        break;
                    case DbProvider.BLToolkit:
                        if (!isBltoolkitPrepared)
                        {
                            PrepareBLToolkit();
                            isBltoolkitPrepared = true;
                        }
                        break;
                    case DbProvider.Linq2db:
                        //((LinqToDB.Data.DataConnection)connection)
                        break;
                    default:
                        break;
                }
            }

            public void Dispose()
            {
                if (Connection != null)
                {
                    switch (dbProvider)
                    {
                        case DbProvider.Npgsql:
                            ((NpgsqlConnection)Connection).Dispose();
                            break;
                        case DbProvider.BLToolkit:
                            ((DbManager)Connection).Dispose();
                            break;
                        case DbProvider.Linq2db:
                            ((LinqToDB.Data.DataConnection)connection).Dispose();
                            break;
                        default:
                            break;
                    }

                    Connection = null;
                }


                GC.SuppressFinalize(this);
            }

            public InnerDbTransaction BeginInnerTransaction()
            {
                return new InnerDbTransaction(this);
            }

        }

        public sealed class InnerDbTransaction : IDisposable
        {
            private DbProvider dbProvider;
            private object transaction;


            public InnerDbTransaction(InnerDbConnection innerConnection)
            {
                DbProvider = innerConnection.DbProvider;

                switch (DbProvider)
                {
                    case DbProvider.Npgsql:
                        transaction = ((NpgsqlConnection)(innerConnection.Connection)).BeginTransaction();
                        break;
                    case DbProvider.BLToolkit:
                        transaction = ((DbManager)(innerConnection.Connection)).BeginTransaction();
                        break;
                    case DbProvider.Linq2db:
                        transaction = ((LinqToDB.Data.DataConnection)innerConnection.Connection).BeginTransaction();
                        break;
                    default:
                        break;
                }
            }

            internal DbProvider DbProvider { get => dbProvider; set => dbProvider = value; }

            public void Commit()
            {
                switch (dbProvider)
                {
                    case DbProvider.Npgsql:
                        ((NpgsqlTransaction)transaction).Commit();
                        break;
                    case DbProvider.BLToolkit:
                        ((DbManagerTransaction)transaction).Commit();
                        break;
                    case DbProvider.Linq2db:
                        ((LinqToDB.Data.DataConnectionTransaction)transaction).Commit();
                        break;
                    default:
                        break;
                }
            }

            public void Dispose()
            {
                if (transaction != null)
                {
                    switch (DbProvider)
                    {
                        case DbProvider.Npgsql:
                            ((NpgsqlTransaction)transaction).Dispose();
                            break;
                        case DbProvider.BLToolkit:
                            ((DbManagerTransaction)transaction).Dispose();
                            break;
                        case DbProvider.Linq2db:
                            ((LinqToDB.Data.DataConnectionTransaction)transaction).Dispose();
                            break;
                        default:
                            break;
                    }
                }

                GC.SuppressFinalize(this);
            }
        }

        //public static void TruncateTable(NpgsqlConnection conn)
        public static void TruncateTable()
        {
            //nlog.Debug("Truncate table...");
            //using (NpgsqlCommand comm = new NpgsqlCommand("TRUNCATE public.user_tbl", conn))
            //{
            //    comm.ExecuteNonQuery();
            //}

            using (DbManager db = GetBLToolkitDbManager())
            {
                db.SetCommand("TRUNCATE public.user_tbl CASCADE").ExecuteNonQuery();
            }
        }

        public enum DbProvider
        {
            Npgsql,
            BLToolkit,
            Linq2db
        }

        public static void PrepareBLToolkit()
        {
            DbManager.AddDataProvider(new BLToolkit.Data.DataProvider.PostgreSQLDataProvider());
            DbManager.AddConnectionString(
                "PostgreSQL",
                "Default",
                GetConnectionString(DbProvider.BLToolkit)
            );
        }

        private static string GenerateConnectionString(DbProvider dbProvider)
        {
            string response = "";
            switch (dbProvider)
            {
                case DbProvider.Npgsql:
                    NpgsqlConnectionStringBuilder connStringBuilder = new NpgsqlConnectionStringBuilder();
                    connStringBuilder.Host = CurrentCredentials.Host;
                    connStringBuilder.Port = CurrentCredentials.Port;
                    connStringBuilder.Username = CurrentCredentials.UserName;
                    connStringBuilder.Password = CurrentCredentials.Password;
                    connStringBuilder.Database = CurrentCredentials.Database;

                    response = connStringBuilder.ToString();
                    break;
                case DbProvider.BLToolkit:
                    response = $"Server={CurrentCredentials.Host};Database={CurrentCredentials.Database};Port={CurrentCredentials.Port};Username={CurrentCredentials.UserName};Password={CurrentCredentials.Password}";

                    break;
                case DbProvider.Linq2db:
                    response =
                        $"Server={CurrentCredentials.Host};Port={CurrentCredentials.Port};Database={CurrentCredentials.Database};User Id={CurrentCredentials.UserName};Password={CurrentCredentials.Password};" +
                        $"Pooling=true;MinPoolSize=10;MaxPoolSize=100;";
                    break;
                default:
                    break;
            }

            return response;
        }

        public static string GetConnectionString(DbProvider dbProvider)
        {
            return connectionStringCache[(int)dbProvider];
        }

        private static DbManager GetBLToolkitDbManager()
        {
            return new DbManager("PostgreSQL", "Default");
        }


        public static List<User> GetUsers(DBHelper.InnerDbConnection connection)
        {
            List<User> userList;

            switch (connection.DbProvider)
            {
                case DbProvider.Npgsql:
                    userList = GetUsersNpgsql((NpgsqlConnection)connection.Connection);
                    break;
                case DbProvider.BLToolkit:
                    userList = GetUsersBLToolkit((DbManager)connection.Connection);
                    break;
                default:
                    userList = GetUsersNpgsql((NpgsqlConnection)connection.Connection);
                    break;
            }



            return userList;
        }

        private static List<User> GetUsersNpgsql(NpgsqlConnection conn)
        {
            List<User> userList = new List<User>();

            using (NpgsqlCommand comm = new NpgsqlCommand("SELECT * FROM public.user_tbl"))
            {
                comm.Connection = conn;
                comm.CommandType = System.Data.CommandType.Text;

                using (NpgsqlDataReader reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        User user = new User();

                        user.Id = reader.GetInt64(0);
                        user.Name = reader.GetString(1);
                        user.Login_count = reader.GetInt32(2);

                        userList.Add(user);
                    }
                }

            }

            return userList;
        }

        private static List<User> GetUsersBLToolkit(DbManager db)
        {
            List<User> userList = new List<User>();

            userList = db.SetCommand("SELECT * FROM public.user_tbl").ExecuteList<User>();

            return userList;
        }

        //private class Linq2DbSettings : ILinqToDBSettings
        //{
        //    public IEnumerable<IDataProviderSettings> DataProviders
        //        => Enumerable.Empty<IDataProviderSettings>();

        //    public string DefaultConfiguration
        //        => "PostgreSQL";
        //    public string DefaultDataProvider
        //        => "PostgreSQL";

        //    public IEnumerable<IConnectionStringSettings> ConnectionStrings
        //    {
        //        get
        //        {
        //            yield return
        //                new ConnectionStringSettings
        //                {
        //                    Name = "PostgreSQL",
        //                    ProviderName = "PostgreSQL",
        //                    ConnectionString = "Server=127.0.0.1;Database=postgres;Port=5432;Username=postgres;Password=123456"
        //                };
        //        }
        //    }
        //}

        public static bool ValidateTable()
        {
            try
            {
                using (DBHelper.InnerDbConnection innerConn = new DBHelper.InnerDbConnection(DBHelper.DbProvider.Npgsql))
                {
                    innerConn.Open();

                    var conn = (NpgsqlConnection)innerConn.Connection;

                    bool isTableExists = false;

                    using (NpgsqlCommand checkComm = new NpgsqlCommand(
                    "SELECT EXISTS( SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_tbl'); "
                    , conn))
                    {
                        isTableExists = (bool)checkComm.ExecuteScalar();
                    }

                    bool isTableHavingTestStruct = false;


                    if (isTableExists)
                    {

                        using (NpgsqlCommand checkComm = new NpgsqlCommand(
                        "SELECT column_name,data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'public' AND table_name = 'user_tbl' ORDER BY ordinal_position; "
                        , conn))
                        {
                            Dictionary<string, string> cols = new Dictionary<string, string>();

                            using (var reader = checkComm.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    cols.Add(reader.GetString(0), reader.GetString(1));
                                }
                            }

                            if (cols.ContainsKey("id") && cols.ContainsKey("name") && cols.ContainsKey("login_count") &&
                                cols["id"].Equals("bigint") &&
                                cols["name"].Equals("character varying") &&
                                cols["login_count"].Equals("integer"))
                            {
                                isTableHavingTestStruct = true;
                            }
                        }

                        if (isTableHavingTestStruct)
                        {
                            return true;
                        }
                        else
                        {
                            nlog.Fatal("The 'user_tbl' table has a structure that is not suitable for tests.");
                            return false;
                        }

                        
                    }
                    else
                    {
                        nlog.Warn("The table 'public.usr_tbl' isn't exist, let's try create this one? [Y/N]");

                        string usr_response = Console.ReadLine();

                        if (usr_response.ToUpper().Equals("Y"))
                        {
                            StringBuilder createCommandBuilder = new StringBuilder();

                            createCommandBuilder.Append(
                                "CREATE SEQUENCE public.user_tbl_id_seq INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1; ");

                            createCommandBuilder.Append(
                                "CREATE TABLE public.user_tbl(id bigint NOT NULL DEFAULT nextval('user_tbl2_id_seq'::regclass),");
                            createCommandBuilder.Append("name character varying COLLATE pg_catalog.\"default\",login_count integer,");
                            createCommandBuilder.Append("CONSTRAINT user_tbl_pkey PRIMARY KEY(id)) WITH ( OIDS = FALSE )");

                            using (NpgsqlCommand createCommand = new NpgsqlCommand(createCommandBuilder.ToString(), conn))
                            {
                                createCommand.ExecuteNonQuery();
                            }

                            using (NpgsqlCommand checkComm = new NpgsqlCommand(
                                "SELECT EXISTS( SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_tbl'); "
                                , conn))
                            {
                                isTableExists = (bool)checkComm.ExecuteScalar();
                            }

                            if (isTableExists)
                            {
                                return true;
                            }
                            else
                            {
                                nlog.Fatal("User_tbl didn't created because of reasons. (I didn't know why, I'm too lazy for this checking on this test code).");
                                return false;
                            }
                        }
                        else
                        {
                            nlog.Fatal("Table 'user_tbl not found and hasn't created!");
                        }
                    }
                }
            }
            catch (Npgsql.PostgresException pgEx)
            {
                nlog.Error(pgEx, "DB Error occured: {0}", pgEx.MessageText);
            }
            catch (Exception ex)
            {
                nlog.Error(ex, "Error occured: {0}", ex.Message);
            }

            return false;
        }
    }
}
