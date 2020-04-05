using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestSqlSpeed_Net.Tests
{
    interface ITester
    {
        void Process();
    }

    public delegate void TestProcessDelegate();
}
