using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Npgsql;
using NLog;
using System.Diagnostics;
using BLToolkit;
using BLToolkit.Data;
using BLToolkit.DataAccess;
using BLToolkit.Reflection;
using TestSqlSpeed_Net.Helpers;

namespace TestSqlSpeed_Net.Tests
{
    class TestProcessor
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public static void ProcessTests()
        {

            ShowUsers();

            List<ITester> testersList = new List<ITester>()
            {
                new Insert.InsertTester(),
                new Select.SelectTester()
            };

            //process all tests
            foreach (var test in testersList)
            {
                test.Process();
            }

            ShowUsers();
        }

        private static void ShowUsers()
        {
            List<User> users = UsersHelper.GetAllUsers();

            Console.WriteLine("BLToolkit returned {0} users from db", users.Count);
        }
    }
}
