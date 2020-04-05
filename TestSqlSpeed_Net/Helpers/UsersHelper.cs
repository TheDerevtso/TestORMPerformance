using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestSqlSpeed_Net.Helpers
{
    class UsersHelper
    {
        public static List<User> CreateRandomUsers(int testusers_count)
        {
            List<User> randomUsersList;

            randomUsersList = new List<User>(testusers_count);

            for (int i = 0; i < testusers_count; i++)
            {
                randomUsersList.Add(User.CreateRandomUser());
            }

            return randomUsersList;
        }

        public static List<User> GetAllUsers()
        {
            List<User> userList;

            using (DBHelper.InnerDbConnection conn = new DBHelper.InnerDbConnection(DBHelper.DbProvider.Npgsql))
            {
                conn.Open();
                userList = DBHelper.GetUsers(conn);
            }

            return userList;
        }

        
    }
}
