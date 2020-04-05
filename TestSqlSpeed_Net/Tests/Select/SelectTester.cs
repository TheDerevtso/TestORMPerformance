using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;
using TestSqlSpeed_Net.Helpers;

namespace TestSqlSpeed_Net.Tests.Select
{
    class SelectTester : ITester
    {
        private static Logger nlog = LogManager.GetCurrentClassLogger();

        public void Process()
        {
            //int testUsersCount = 50000;

            //nlog.Trace("Create {0} random users...", testUsersCount);
            //List<User> randomUsersList = Helpers.UsersHelper.CreateRandomUsers(testUsersCount);
            //nlog.Trace("Random user array created");

            List<User> halfOfUsers = UsersThanos(UsersHelper.GetAllUsers());



            // declare all test types
            List<SelectTest> insertTests = new List<SelectTest>()
            {                
                new SelectBLToolkitTest(),
                new SelectLinq2dbTest(),
                new SelectNpgsqlTest(),


            };

            //process all tests
            foreach (var test in insertTests)
            {
                nlog.Warn("Start testing {0}", test.GetLibraryVersion(test.DbProviderType));
                test.Start(halfOfUsers);
            }

        }

        private static Random random = new Random();

        private List<User> UsersThanos(List<User> allUsers)
        {
            List<User> resultList = new List<User>();

            int numOfSelected = allUsers.Count / 2;

            nlog.Trace("Thanos snap! Select unique random {0} users...", numOfSelected);

            //random code: https://codereview.stackexchange.com/questions/61338/generate-random-numbers-without-repetitions 

            HashSet<int> elementNums = new HashSet<int>();

            for (int i = 0; i < numOfSelected; i++)
            {
                while (!elementNums.Add(random.Next(allUsers.Count))) ;

                resultList.Add(allUsers[elementNums.Last()]);
            }

            return resultList;
        }
    }
}
