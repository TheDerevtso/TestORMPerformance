using System;
using System.Linq;
using System.Collections.Generic;

using LinqToDB.DataProvider.SqlServer;
using LinqToDB.DataProvider.PostgreSQL;
using LinqToDB.Mapping;

namespace TestSqlSpeed_Net
{
    [Table(Name = "user_tbl")]
    public class Usr
    {
        [Column(Name = "id")]
        public long Id;
        [Column(Name = "name")]
        public string Name;
        [Column(Name = "login_count")]
        public int LoginCount;
    }

    class TestShit
    {
        //const string ConnectionString =
        //    "Data Source=.;Database=Northwind;Integrated Security=SSPI";

        readonly static string ConnectionString = Helpers.DBHelper.GetConnectionString(Helpers.DBHelper.DbProvider.Linq2db);

        public static void Test()
        {
            List<Usr> users = new List<Usr>();

            using (var db = PostgreSQLTools.CreateDataConnection(ConnectionString))
            {
                var q =
                    from c in db.GetTable<Usr>()
                    select c;

                users.AddRange(q);
                //foreach (var user in q)
                //{
                //    Console.WriteLine("ID : {0}, Name : {1}",
                //        user.id,
                //        user.name);
                //}
            }
            Console.WriteLine("Linq2db users count: {0}", users.Count);
        }
    }
}