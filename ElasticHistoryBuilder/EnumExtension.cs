namespace ElasticLogBuilder
{
    using System;
    using System.Linq;
    using System.Reflection;

    public static class EnumExtension
    {
        /// <summary>
        /// Возвращает значение атрибута у значения перечисления
        /// </summary>
        public static TAttribute GetAttribute<TAttribute>(this Enum enumValue)
                where TAttribute : Attribute
        {
            return enumValue.GetType()
                            .GetMember(enumValue.ToString())
                            .First()
                            .GetCustomAttribute<TAttribute>();
        }

        public static bool TryGetEnumType(this Type type, out Type enumType)
        {
            bool isEnum = type.IsEnum;

            if (!type.IsEnum)
            {
                enumType = Nullable.GetUnderlyingType(type);
                isEnum = enumType != null && enumType.IsEnum;
            }
            else
            {
                enumType = type;
            }

            return isEnum;
        }
    }
}
