using System;

namespace ElasticHistoryService.Model
{
    public static class ExceptionExtensions
    {
        /// <summary>
        /// Получение внутреннего исключения
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static Exception GetOriginalException(this Exception ex)
        {
            if (ex.InnerException == null)
                return ex;

            return ex.InnerException.GetOriginalException();
        }
    }
}
