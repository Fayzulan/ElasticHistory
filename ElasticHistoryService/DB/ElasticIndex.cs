using Nest;

namespace ElasticHistoryService.DB
{
    /// <summary>
    /// Базовый объект индекса
    /// </summary>
    public abstract class ElasticIndex
    {
        /// <summary>
        /// Id индекса
        /// </summary>
        [Ignore]
        public string Id { get; set; }

        /// <summary>
        /// Наименование индекса
        /// </summary>
        [Ignore]
        public string IndexName { get; set; }
    }
}
