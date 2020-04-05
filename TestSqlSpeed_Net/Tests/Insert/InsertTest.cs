using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestSqlSpeed_Net.Helpers;
using System.Diagnostics;
using NLog;

namespace TestSqlSpeed_Net.Tests.Insert
{
    public abstract class InsertTest
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();
        protected List<User> UserList;

        protected struct Insert_TestParameters
        {
            public Insert_TestDelegate TestProcedure;
            public bool UseTransaction;
            public bool OneByOne;
        }

        public void Start(List<User> userList)
        {
            this.UserList = userList;

            using (DBHelper.InnerDbConnection connection = new DBHelper.InnerDbConnection(DbProviderType))
            {
                List<Insert_TestParameters> parametersList = CreateParametersVariations();

                connection.Open();

                foreach (var parameter in parametersList)
                {
                    DBHelper.TruncateTable();

                    MeasureTest_Insert(userList, connection, parameter);
                }

            }
        }

        protected abstract List<Insert_TestParameters> CreateParametersVariations();

        protected delegate void Insert_TestDelegate(DBHelper.InnerDbConnection connection, List<User> userList, Insert_TestParameters parameters);

        public abstract DBHelper.DbProvider DbProviderType { get; }


        //protected abstract void ProcessInnerTests(List<User> userList);

        public string GetLibraryVersion(DBHelper.DbProvider dbProvider)
        {
            string response = ""; ;
            switch (dbProvider)
            {
                case DBHelper.DbProvider.Npgsql:

                    if (System.IO.File.Exists("Npgsql.dll"))
                    {
                        response = String.Format("Npgsql_v{0}", FileVersionInfo.GetVersionInfo("Npgsql.dll").FileVersion);
                    }
                    break;
                case DBHelper.DbProvider.BLToolkit:
                    if (System.IO.File.Exists("BLToolkit.4.dll"))
                    {
                        response = String.Format("BLToolkit_v{0}", FileVersionInfo.GetVersionInfo("BLToolkit.4.dll").FileVersion);
                    }
                    break;
                default:
                    break;
            }

            return response;
        }

        protected void MeasureTest_Insert(List<User> randomUsersList, DBHelper.InnerDbConnection innerConnection, Insert_TestParameters testParameters)
        {

            Insert_TestDelegate MethodForTest = testParameters.TestProcedure;
            bool useTransaction = testParameters.UseTransaction;
            bool oneByOne = testParameters.OneByOne;

            nlog.Info("{3}: test {0} ({1}, {2})", MethodForTest.Method.Name, useTransaction ? "using transaction" : "---", oneByOne ? "one-by-one" : "BATCH", GetLibraryVersion(innerConnection.DbProvider));


            Stopwatch sw = new Stopwatch();

            sw.Start();

            DBHelper.InnerDbTransaction transaction = null;

            try
            {
                if (useTransaction)
                {
                    transaction = innerConnection.BeginInnerTransaction();
                }


                MethodForTest(innerConnection, randomUsersList, testParameters);


                if (useTransaction && transaction != null)
                {
                    transaction.Commit();
                }
            }
            finally
            {

                if (useTransaction && transaction != null)
                {
                    transaction.Dispose();
                }

            }

            sw.Stop();

            long forEachElapsedMs = sw.ElapsedMilliseconds;

            nlog.Debug("{0}: {1} Users inserted on {2}ms", MethodForTest.Method.Name, randomUsersList.Count, forEachElapsedMs);
        }
    }
}
