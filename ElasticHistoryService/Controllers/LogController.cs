using ElasticLogBuilder;
using ElasticHistoryService.Dto;
using ElasticHistoryService.Dto.Response;
using ElasticHistoryService.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ElasticHistoryService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LogController : ControllerBase
    {
        const string PostLogError = "Ошибка записи логов";
        private readonly ILogger<LogController> _logger;
        private readonly IDBLogger _DBLogger;

        public LogController(IDBLogger DBLogger, ILogger<LogController> logger)
        {
            _logger = logger;
            _DBLogger = DBLogger;
        }

        /// <summary>
        /// Запись логов
        /// </summary>
        /// <param name="requestLogs"></param>
        /// <returns></returns>
        [HttpPost]
        public async Task<ServiceResponseDto> PostLog([FromBody] List<ElasticLogRequestDto> requestLogs)
        {
            try
            {
                if (requestLogs.Count > 0)
                {
                    _logger.LogInformation($"Запрос на логирование объектов: {requestLogs.Count}");
                    await _DBLogger.PostAsync(requestLogs);
                }
                else
                {
                    _logger.LogInformation($"Запрос на логирование объектов: 0");
                }

                return new ServiceResponseDto();
            }
            catch (Exception ex)
            {
                return new ServiceResponseDto { Success = false, Message = PostLogError };
            }
        }

        /// <summary>
        /// Получение логов
        /// </summary>
        /// <param name="baseParams"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<ClientResponseDto> List([FromQuery] BaseParamsDto baseParams)
        {
            try
            {
                ClientListResponseDto<LogResponseDto> data = await _DBLogger.GetListAsync(baseParams);
                return data;
            }
            catch (Exception ex)
            {
                Exception error = ex.GetOriginalException();
                string message = $"Ошибка получения логов";
                _logger.LogError(error, message);
                return new ClientResponseDto { Success = false, Message = message };
            }
        }      

        /// <summary>
        /// Получение истории лога
        /// </summary>
        /// <param name="baseGetParams"></param>
        /// <returns></returns>
        [HttpGet("{id}")]
        public async Task<ClientResponseDto> GetLog(string id)
        {
            try
            {
                ClientGetResponseDto<LogHistoryDto> data = await _DBLogger.GetAsync(id);
                return data;
            }
            catch (Exception ex)
            {
                Exception error = ex.GetOriginalException();
                string message = $"Ошибка получения лога";
                _logger.LogError(error, message);
                return new ClientResponseDto { Success = false, Message = message };
            }
        }
    }
}
