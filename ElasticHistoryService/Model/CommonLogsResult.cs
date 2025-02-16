using ElasticHistoryService.DB;
using System.Collections.Generic;

namespace ElasticHistoryService.Model
{
    public class CommonLogsResult
    {
        public List<ElasticLog> ElasticLogs {  get; set; }

        public long Count { get; set; }
    }
}
