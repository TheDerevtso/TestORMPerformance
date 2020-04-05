using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BLToolkit.Data;
using BLToolkit.DataAccess;

namespace TestSqlSpeed_Net
{
    public abstract class UserAccessor : DataAccessor
    {
        public abstract List<User> GetUserListByName(
            long id, string name, int login_count);

        public abstract User GetPersonById(int id);

        public abstract void DeletePersonById(int id);
    }
}
