using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;

namespace TestSqlSpeed_Net.Tests.Insert
{
    class InsertTester : ITester
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public void Process()
        {
            int testUsersCount = 50000;
            //int testUsersCount = 5000;
            

            nlog.Trace("Create {0} random users...", testUsersCount);
            List<User> randomUsersList = Helpers.UsersHelper.CreateRandomUsers(testUsersCount);
            nlog.Trace("Random user array created");


            // declare all types of tests
            List<InsertTest> insertTests = new List<InsertTest>()
            {
                //new InsertBLToolkitTest(),
                new InsertNpgsqlTest(),

            };

            //process all tests
            foreach (var test in insertTests)
            {
                nlog.Warn("Start testing {0}", test.GetLibraryVersion(test.DbProviderType));
                test.Start(randomUsersList);
            }



        }
    }
}
