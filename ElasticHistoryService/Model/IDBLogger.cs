using ElasticLogBuilder;
using ElasticHistoryService.Dto;
using ElasticHistoryService.Dto.Response;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ElasticHistoryService.Model
{
    /// <summary>
    /// Работа с логами
    /// </summary>
    public interface IDBLogger
    {
        /// <summary>
        /// Запись логов
        /// </summary>
        /// <param name="requestLogs"></param>
        Task PostAsync(List<ElasticLogRequestDto> requestLogs);

        /// <summary>
        /// Получение списка логов
        /// </summary>
        /// <param name="baseParams"></param>
        /// <returns></returns>
        Task<ClientListResponseDto<LogResponseDto>> GetListAsync(BaseParamsDto baseParams);

        /// <summary>
        /// Получение истории лога
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task<ClientGetResponseDto<LogHistoryDto>> GetAsync(string id);
    }
}
