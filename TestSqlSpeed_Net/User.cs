using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BLToolkit.Mapping;
using BLToolkit.DataAccess;
using BLToolkit;
using LinqToDB.Mapping;

namespace TestSqlSpeed_Net
{
    #region UserClass

    [Table(Name = "user_tbl")]
    /// <summary>
    /// Simple class with simple data: <see cref="User.Name"/> string and integer <see cref="User.Login_count"/>
    /// </summary>
    public class User
    {
        [Column(Name = "id", IsPrimaryKey = true, IsIdentity = true)]
        public long Id;
        [Column(Name = "name")]
        public string Name;
        [Column(Name = "login_count")]
        public int Login_count;

        public override string ToString()
        {
            return
                (new StringBuilder())
                .AppendFormat("Id: '{0}', Name: '{1}', Login_Count='{2}'",
                Id, Name, Login_count)
                .ToString();
        }

        private static Random random = new Random();

        public static User CreateRandomUser()
        {
            return (new User() { Name = RandomString(8, false), Login_count = random.Next(5000) });
        }

        private static String RandomString(int size, bool lowerCase)
        {
            StringBuilder builder = new StringBuilder();

            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                builder.Append(ch);
            }
            if (lowerCase)
                return builder.ToString().ToLower();
            return builder.ToString();
        }
    }
    #endregion
}
