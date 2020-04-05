using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Npgsql;
using NLog;
using TestSqlSpeed_Net.Helpers;

namespace TestSqlSpeed_Net
{

    class Program
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            //nlog.Warn("Test shit");
            //TestShit.Test();

            nlog.Warn("ORM Testing");

            DBHelper.CurrentCredentials = new DBHelper.InnerDbCredentials()
            {
                Host = "127.0.0.1",
                Port = 5432,
                UserName = "postgres",
                Password = "123456",
                Database = "postgres",
            };

            if (DBHelper.ValidateCredentials() && DBHelper.ValidateTable())
            {
                DBHelper.PrepareBLToolkit();
                Tests.TestProcessor.ProcessTests();
            }
            else
            {
                nlog.Fatal("Tests will not working with wrong credentials or without 'user_tbl' table");
            }



            nlog.Warn("Press any key to exit...");
            Console.ReadKey();
        }

    }
}
