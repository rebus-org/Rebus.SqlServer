#if NETSTANDARD1_6
using System;
using System.Reflection;

namespace Rebus.SqlServer
{
    static class Shims
    {
        public static PropertyInfo GetProperty(this Type type, string name)
        {
            return type.GetTypeInfo().GetProperty(name);
        }

        public static bool IsInstanceOfType(this Type type, object o)
        {
            return type.GetTypeInfo().IsInstanceOfType(o);
        }
    }
}
#endif