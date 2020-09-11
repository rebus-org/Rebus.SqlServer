namespace Rebus.SqlServer
{
    class Reflect
    {
        public static object Value(object obj, string path)
        {
            var dots = path.Split('.');

            foreach(var dot in dots)
            {
                var propertyInfo = obj.GetType().GetProperty(dot);
                if (propertyInfo == null) return null;
                obj = propertyInfo.GetValue(obj, new object[0]);
                if (obj == null) break;
            }

            return obj;
        }
    }
}
