using ElasticLogBuilder;
using ElasticHistoryService.DB;
using ElasticHistoryService.Dto;
using ElasticHistoryService.Dto.Response;
using Microsoft.Extensions.Logging;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.ObjectModel;

namespace ElasticHistoryService.Model
{
    public class ElasticLogger : IDBLogger
    {
        const int DefaultResultSize = 500;
        private ElasticClient _client;
        private readonly ILogger<ElasticLogger> _logger;
        private const string LogIndexName = "log";
        public static Semaphore sim = new Semaphore(20, 20);

        public ElasticLogger(ElasticClient client, ILogger<ElasticLogger> logger)
        {
            _client = client;
            _logger = logger;
        }

        #region public methods
        public async Task<ClientGetResponseDto<LogHistoryDto>> GetAsync(string id)
        {
            var resultLog = new ResultLog<RawLogMutation>();

            ElasticLog log = await TryGetElasticLogAsync(LogIndexName, id);

            if (log == null)
            {
                string message = $"Главный лог {id} не найден";
                return new ClientGetResponseDto<LogHistoryDto> { Success = false, Message = message };
            }

            ElasticLogRawData logData = await GetLogRawDataAsync(log);

            if (logData == null)
            {
                string message = $"Лог данных для главного лога {id} не найден";
                return new ClientGetResponseDto<LogHistoryDto> { Success = false, Message = message };
            }

            List<RawLogMutation> newLogDataMutation;

            if (log.ActionType == ElasticLogBuilder.ActionType.Delete)
            {
                newLogDataMutation = logData.JsonData.Select(x => new RawLogMutation
                {
                    PropertyName = x.Value.Name,
                    PropertyValueOld = x.Value.Value.Name
                }).ToList();
            }
            else
            {
                newLogDataMutation = logData.JsonData.Select(x => new RawLogMutation
                {
                    PropertyName = x.Value.Name,
                    PropertyValueNew = x.Value.Value.Name
                }).ToList();
            }

            resultLog.CommonLog = log;
            resultLog.LogHistory = newLogDataMutation.ToList();

            if (log.ActionType == ElasticLogBuilder.ActionType.Edit)
            {
                ElasticLog oldLog = await GetOldLogAsync(log);

                if (oldLog != null)
                {
                    ElasticLogRawData oldLogData = await GetLogRawDataAsync(oldLog);

                    if (oldLogData != null)
                    {
                        List<RawLogMutation> oldLogDataMutation = oldLogData.JsonData.Select(x => new RawLogMutation
                        {
                            PropertyName = x.Value.Name,
                            PropertyValueOld = x.Value.Value.Name
                        }).ToList();

                        List<RawLogMutation> logHistory = oldLogDataMutation.Join(newLogDataMutation, x => x.PropertyName, y => y.PropertyName, (x, y) => new RawLogMutation
                        {
                            PropertyName = y.PropertyName,
                            PropertyValueOld = x.PropertyValueOld,
                            PropertyValueNew = y.PropertyValueNew
                        }).ToList();

                        //учитывать добавленые значения
                        AddMissingFields(newLogDataMutation, ref logHistory);

                        //учитывать удаленные значения
                        AddMissingFields(oldLogDataMutation, ref logHistory);

                        resultLog.LogHistory = logHistory.OrderBy(x => x.PropertyName).ToList();
                    }
                }
            }

            return new ClientGetResponseDto<LogHistoryDto>
            {
                Data = new LogHistoryDto
                {
                    Id = resultLog.CommonLog.Id,
                    ActionType = resultLog.CommonLog.ActionType,
                    EntityId = resultLog.CommonLog.EntityId,
                    EntityType = resultLog.CommonLog.EntityType,
                    ObjectCreateDate = resultLog.CommonLog.ObjectCreateDate,
                    UserLogin = resultLog.CommonLog.UserLogin,
                    Operator = resultLog.CommonLog.Operator,
                    BuisnesComment = resultLog.CommonLog.BuisnesComment,
                    LogHistory = resultLog.LogHistory
                }
            };
        }
        
        public async Task<ClientListResponseDto<LogResponseDto>> GetListAsync(BaseParamsDto baseParams)
        {
            List<LogResponseDto> dblogs;
            StoreLoadParams storeParams = baseParams.GetStoreLoadParams();

            if (baseParams.subdivisionId > 0)
            {
                storeParams.filter.Add(new FilterColumn { @operator = "eq", property = "subdivisionid", value = baseParams.subdivisionId.ToString() });
            }

            CommonLogsResult commonLogsResult = await TryGetCommonLogsAsync(storeParams, null, storeParams.start, storeParams.limit);

            if (commonLogsResult != null)
            {
                dblogs = commonLogsResult.ElasticLogs.Select(d => new LogResponseDto
                {
                    Id = d.Id,
                    ActionType = d.ActionType,
                    EntityId = d.EntityId,
                    EntityType = d.EntityType,
                    ObjectCreateDate = d.ObjectCreateDate,
                    UserLogin = d.UserLogin,
                    Operator = d.Operator,
                    BuisnesComment = d.BuisnesComment
                }).ToList();

                return new ClientListResponseDto<LogResponseDto> { Data = dblogs, TotalCount = commonLogsResult.Count };
            }
            else
            {
                dblogs = new List<LogResponseDto>();
                return new ClientListResponseDto<LogResponseDto> { Data = dblogs, TotalCount = 0, Success = false, Message = $"Ошибка получения логов" };
            }
        }

        public async Task PostAsync(List<ElasticLogRequestDto> requestLogs)
        {
            foreach (ElasticLogRequestDto requestLog in requestLogs)
            {
                string indexName = requestLog.EntityTypeCode.ToLower();

                if (!_client.Indices.Exists(Indices.Index(indexName)).Exists)
                {
                    _logger.LogInformation("Создание нового индекса {indexName}");

                    var rawLogCreateIndexResponse = _client.Indices.Create(indexName, c => c
                        .Map<ElasticLogData<object>>(m => m.AutoMap()));

                    if (!rawLogCreateIndexResponse.IsValid)
                    {
                        _logger.LogError($"Ошибка создания индекса {indexName}: {rawLogCreateIndexResponse.ServerError?.Error?.Reason}");
                    }
                }
            }

            await Task.Run(() => SaveLogsAsync(requestLogs));
        }
        #endregion

        #region private methods
        /// <summary>
        /// Сохранение логов в бд
        /// </summary>
        /// <param name="requestLogs"></param>
        private async Task SaveLogsAsync(List<ElasticLogRequestDto> requestLogs)
        {
            try
            {
#if DEBUG
                sim.WaitOne();
#endif
                string entityTypes = string.Join(", ", requestLogs.GroupBy(r => new { r.DatabaseName, r.EntityTypeCode }).Select(r => r.First()).Select(r => $"{ r.DatabaseName}.{r.EntityTypeCode}"));
                _logger.LogInformation($"Запись логов для объектов {entityTypes}");
                var elasticLogs = new Dictionary<ElasticLog, Dictionary<string, ElasticLogFieldRequestDto>>();
                var elasticRawLogs = new List<ElasticLogRawData>();
                string requestId = Guid.NewGuid().ToString();

                foreach (ElasticLogRequestDto requestLog in requestLogs)
                {
                    if (string.IsNullOrEmpty(requestLog.EntityTypeCode))
                    {
                        _logger.LogError($"Не указан код таблицы");
                        continue;
                    }

                    var logId = Guid.NewGuid().ToString().ToLower();
                    var log = new ElasticLog
                    {
                        Id = logId,
                        ActionType = requestLog.ActionType,
                        ObjectCreateDate = requestLog.CreatedDate,
                        DatabaseName = requestLog.DatabaseName,
                        EntityId = requestLog.EntityId,
                        EntityType = requestLog.EntityType,
                        EntityTypeCode = requestLog.EntityTypeCode,
                        UserLogin = requestLog.UserLogin,
                        Operator = requestLog.Operator,
                        SubdivisionId = requestLog.SubdivisionId,
                        BuisnesComment = requestLog.BuisnesComment,
                        IndexName = LogIndexName,
                        RequestId = requestId
                    };

                    bool anyChanges = await AnyChangesAsync(log, requestLog);

                    if (anyChanges)
                    {
                        elasticLogs.Add(log, requestLog.JsonData);
                    }
                }

                foreach (KeyValuePair<ElasticLog, Dictionary<string, ElasticLogFieldRequestDto>> log in elasticLogs)
                {
                    var rawLogId = Guid.NewGuid().ToString().ToLower();
                    var jsonData = new Dictionary<string, ElasticLogField>();

                    foreach (KeyValuePair<string, ElasticLogFieldRequestDto> field in log.Value)
                    {
                        if (field.Value.Value.Name == null)
                        {
                            continue;
                        }

                        var fieldValue = new ElasticLogLinkValue { Name = field.Value.Value.Name };

                        //если есть ссылка на оригинальную сущность, то ищем ссылку на залогируемую сущность
                        if (field.Value.Value.Id > 0)
                        {
                            fieldValue.Id = field.Value.Value.Id;

                            if (field.Value.Value.EntityCode != null && field.Value.Value.EntityCode != ElasticLogBuilder.ElasticLogBuilder.EnumEntityCode)
                            {
                                //поиск ссылочного объекта в одном из сохраняемых логах текущего запроса
                                ElasticLog linkElasticLog = elasticLogs.Keys.Where(e => e.EntityTypeCode == field.Value.Value.EntityCode && e.EntityId == field.Value.Value.Id).FirstOrDefault();

                                //если ссылочный объект нашелся
                                if (linkElasticLog != null)
                                {
                                    fieldValue.LogId = linkElasticLog.Id;
                                }
                                else
                                {
                                    //иначе ищем в последний лог записи
                                    ElasticLog linkLog;
                                    var searchOldLogSearchDescripter = new SearchDescriptor<ElasticLog>()
                                            .Index(LogIndexName)
                                            .Size(1)
                                            .Query(q => q
                                                .Bool(b => b
                                                    .Must(m => m.Term(t => t.EntityId, field.Value.Value.Id)
                                                        && m.Term(t => t.EntityTypeCode, field.Value.Value.EntityCode))
                                                )
                                            )
                                            .Sort(s => s.Field(f => f.ObjectCreateDateString, SortOrder.Descending));

                                    linkLog = await TryGetElasticDocumentAsync(searchOldLogSearchDescripter);

                                    if (linkLog != null)
                                    {
                                        fieldValue.LogId = linkLog.Id;
                                    }
                                }
                            }
                        }

                        jsonData.Add(field.Key, new ElasticLogField { Name = field.Value.Name, Value = fieldValue });
                    }

                    var rawLog = new ElasticLogRawData
                    {
                        Id = rawLogId,
                        LogId = log.Key.Id,
                        IndexName = log.Key.EntityTypeCode.ToLower(),
                        JsonData = jsonData
                    };
                    elasticRawLogs.Add(rawLog);
                }

                if (elasticLogs.Count > 0)
                {
                    var logBulk = new BulkDescriptor();
                    logBulk.Index(LogIndexName);
                    logBulk.IndexMany<ElasticLog>(elasticLogs.Keys, (descriptor, log) => descriptor.Index(log.IndexName).Id(log.Id));
                    BulkResponse logResponse = await _client.BulkAsync(b => logBulk);

                    if (logResponse.Errors)
                    {
                        foreach (var itemWithError in logResponse.ItemsWithErrors)
                        {
                            _logger.LogError($"Ошибка записи основного лога {itemWithError.Id}: {itemWithError.Error?.Reason}");
                        }
                    }
                }

                if (elasticRawLogs.Count > 0)
                {
                    var rawLogBulk = new BulkDescriptor();
                    rawLogBulk.IndexMany<ElasticLogRawData>(elasticRawLogs, (descriptor, log) => descriptor.Index(log.IndexName).Id(log.Id));
                    BulkResponse rawResponse = await _client.BulkAsync(b => rawLogBulk);

                    if (rawResponse.Errors)
                    {
                        foreach (var itemWithError in rawResponse.ItemsWithErrors)
                        {
                            _logger.LogError($"Ошибка записи лога с данными {itemWithError.Id}: {itemWithError.Error?.Reason}");
                        }
                    }
                }

                _logger.LogInformation($"Запись логов для объектов {entityTypes} успешно завершено");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Ошибка записи лога : {ex.GetOriginalException().Message}");
            }
            finally
            {
#if DEBUG
                sim.Release();
#endif
            }
        }

        /// <summary>
        /// Есть ли отличия  по сравнению с предыдущей записью
        /// </summary>
        /// <param name="log"></param>
        /// <param name="requestLog"></param>
        /// <returns></returns>
        private async Task<bool> AnyChangesAsync(ElasticLog log, ElasticLogRequestDto requestLog)
        {
            try
            {
                ElasticLog oldLog = await GetOldLogAsync(log);

                if (log.ActionType == ElasticLogBuilder.ActionType.Edit)
                {
                    if (oldLog != null)
                    {
                        ElasticLogRawData oldLogData = await GetLogRawDataAsync(oldLog);

                        if (oldLogData != null)
                        {
                            foreach (KeyValuePair<string, ElasticLogFieldRequestDto> newLogfield in requestLog.JsonData)
                            {
                                ElasticLogField oldLogField;

                                //Старое значение пустое, а новое - не пустое
                                if (!oldLogData.JsonData.TryGetValue(newLogfield.Key, out oldLogField) && newLogfield.Value.Value.Name != null)
                                {
                                    return true;
                                }

                                if (oldLogField != null)
                                {
                                    //Изменилось наименование свойства
                                    if (oldLogField.Name != newLogfield.Value.Name)
                                    {
                                        return true;
                                    }

                                    //Изменились ссылка
                                    if (oldLogField.Value.Id != newLogfield.Value.Value.Id)
                                    {
                                        return true;
                                    }

                                    //У не ссылочного свойства изменилось значение
                                    if (oldLogField.Value.Id == 0 && oldLogField.Value.Name != newLogfield.Value.Value.Name)
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                        else
                        {
                            return true;
                        }
                    }
                    else
                    {
                        return true;
                    }

                }
                else if (log.ActionType == ElasticLogBuilder.ActionType.Actualization)
                {
                    return oldLog == null;
                }
                else
                {
                    return true;
                }

            }
            catch (Exception ex)
            {
                _logger.LogError($"Ошибка поиска наличия изменений: {ex.GetOriginalException().Message}");
            }

            return false;
        }

        /// <summary>
        /// Получение лога данных
        /// </summary>
        /// <param name="logDataIndexName">Индекс лога данных</param>
        /// <param name="logId">id документа в в главном логе</param>
        /// <returns></returns>
        private async Task<T> TryGetElasticDocumentAsync<T>(SearchDescriptor<T> searchDescripter) where T : ElasticIndex
        {
            var searchLogData = await _client.SearchAsync<T>(s => searchDescripter);
            T logData = null;

            if (searchLogData.IsValid)
            {
                var searchLogDataResult = searchLogData.Hits.FirstOrDefault();
                logData = searchLogDataResult?.Source;

                if (logData != null)
                {
                    logData.Id = searchLogDataResult.Id;
                }
            }
            else
            {
                string message = $"Ошибка поиска";
                _logger.LogError($"{message}: {searchLogData.ServerError?.Error?.Reason}. {searchLogData.ServerError?.Error?.RootCause?.FirstOrDefault()?.Reason}");
            }

            return logData;
        }

        /// <summary>
        /// Получение главного лога
        /// </summary>
        /// <param name="logIndexName">Индекс главного лога</param>
        /// <param name="logId">id документа в в главном логе</param>
        /// <returns>главный лог</returns>
        private async Task<ElasticLog> TryGetElasticLogAsync(string logIndexName, string logId)
        {
            ElasticLog log = null;
            GetResponse<ElasticLog> logResponse = await _client.GetAsync<ElasticLog>(logId, g => g.Index(logIndexName));

            if (logResponse.IsValid && logResponse.Source != null)
            {
                log = logResponse.Source;
                log.Id = logResponse.Id;
            }

            string message = $"Ошибка получения лога {logId}";
            _logger.LogError($"{message}: {logResponse.ServerError?.Error}");
            return log;
        }

        private List<string> GetTableList()
        {
            var tableList = new List<string>();
            //http://localhost:9200/_aliases
            return tableList;
        }

        /// <summary>
        /// Формирует сортировку
        /// </summary>
        /// <param name="sorters"></param>
        /// <returns></returns>
        private SortDescriptor<ElasticLog> GetSortDescriptor(List<OrderColumn> sorters)
        {
            SortDescriptor<ElasticLog> sortDescriptor = new SortDescriptor<ElasticLog>();

            foreach (var sort in sorters)
            {
                var sortOrder = SortOrder.Descending;

                if (sort.direction == "ASC")
                {
                    sortOrder = SortOrder.Ascending;
                }

                switch (sort.property.ToLower())
                {
                    case "entityid":
                        sortDescriptor.Field(f => f.Field(p => p.EntityId).Order(sortOrder).Mode(SortMode.Min));
                        break;
                    case "actiontype":
                        sortDescriptor.Field(f => f.Field(p => p.ActionType).Order(sortOrder).Mode(SortMode.Min));
                        break;
                    case "entitytype":
                        sortDescriptor.Field(f => f.Field(p => p.EntityType).Order(sortOrder).Mode(SortMode.Min));
                        break;
                    case "userlogin":
                        sortDescriptor.Field(f => f.Field(p => p.UserLogin).Order(sortOrder).Mode(SortMode.Min));
                        break;
                    case "operator":
                        sortDescriptor.Field(f => f.Field(p => p.Operator).Order(sortOrder).Mode(SortMode.Min));
                        break;
                }

            }

            var objectcreatedateSort = sorters.Where(s => s.property.ToLower() == "objectcreatedate").FirstOrDefault();
            var objectcreatedateSortOrder = SortOrder.Descending;

            if (objectcreatedateSort != null && objectcreatedateSort.direction == "ASC")
            {
                objectcreatedateSortOrder = SortOrder.Ascending;
            }

            sortDescriptor.Field(f => f.ObjectCreateDateString, objectcreatedateSortOrder);
            return sortDescriptor;
        }

        /// <summary>
        /// Получение общих логов
        /// </summary>
        /// <param name="baseParams"></param>
        /// <param name="includeEntyties"></param>
        /// <returns></returns>
        private async Task<CommonLogsResult> TryGetCommonLogsAsync(StoreLoadParams storeParams, List<string> orIds, int start, int size, bool needAggregationCountByRequest = false)
        {
            CommonLogsResult commonLogsResult = null;
            //todo: передать только фильтры по индексу log
            Func<BoolQueryDescriptor<ElasticLog>, IBoolQuery> boolQuery = GetElasticBoolQuery(storeParams.filter);
            Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> querySelector;

            if (orIds != null && orIds.Count > 0)
            {
                var shouldQueries = new List<Func<QueryContainerDescriptor<ElasticLog>, QueryContainer>>();
                Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> orIdsQuery = (q) => q.Ids(s => s.Values(orIds));
                shouldQueries.Add(orIdsQuery);

                if (boolQuery != null)
                {
                    Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> filterQuery = (q) => q.Bool(boolQuery);
                    shouldQueries.Add(filterQuery);
                }

                Func<BoolQueryDescriptor<ElasticLog>, IBoolQuery> parentBoolQuery = (bq) => bq.Should(shouldQueries);
                querySelector = (q) => q.Bool(parentBoolQuery);
            }
            else
            {
                if (boolQuery == null)
                {
                    querySelector = (q) => q.MatchAll();
                }
                else
                {
                    querySelector = (q) => q.Bool(boolQuery);
                }
            }

            SortDescriptor<ElasticLog> sortDescriptor = GetSortDescriptor(storeParams.sort);
            var searchDescriptor = new SearchDescriptor<ElasticLog>();
            searchDescriptor.Index(LogIndexName)
                .Query(querySelector)
                .Sort(s => sortDescriptor);

            if (needAggregationCountByRequest)
            {
                searchDescriptor.Size(DefaultResultSize);
                searchDescriptor.Aggregations(a => a.Cardinality("request_id", t => t.Field(f => f.RequestId)));
            }
            else
            {
                searchDescriptor.From(start).Size(size);
            }

            ISearchResponse<ElasticLog> searchResponse = await _client.SearchAsync<ElasticLog>(s => searchDescriptor);

            if (searchResponse.HitsMetadata != null)
            {
                commonLogsResult = new CommonLogsResult();

                if (needAggregationCountByRequest)
                {
                    commonLogsResult.Count = (long)searchResponse.Aggregations.Cardinality("request_id").Value;
                }
                else
                {
                    var countResponse = _client.Count<ElasticLog>(s => s.Index(LogIndexName).Query(querySelector));
                    commonLogsResult.Count = countResponse.Count;
                }

                IEnumerable<ElasticLog> elasticLogE = searchResponse.HitsMetadata.Hits.Select(s => new ElasticLog
                {
                    Id = s.Id,
                    ActionType = s.Source.ActionType,
                    BuisnesComment = s.Source.BuisnesComment,
                    DatabaseName = s.Source.DatabaseName,
                    EntityId = s.Source.EntityId,
                    EntityType = s.Source.EntityType,
                    EntityTypeCode = s.Source.EntityTypeCode,
                    IndexName = s.Source.IndexName,
                    ObjectCreateDate = s.Source.ObjectCreateDate,
                    Operator = s.Source.Operator,
                    RequestId = s.Source.RequestId,
                    SubdivisionId = s.Source.SubdivisionId,
                    UserLogin = s.Source.UserLogin
                }).AsEnumerable();

                if (needAggregationCountByRequest)
                {
                    commonLogsResult.ElasticLogs = elasticLogE.Skip(start).Take(size).ToList();
                }
                else
                {
                    commonLogsResult.ElasticLogs = elasticLogE.ToList();
                }
            }
            else
            {
                if (searchResponse.ServerError?.Error?.Reason != null)
                {
                    _logger.LogError($"Ошибка поиска документов: {searchResponse.ServerError?.Error?.Reason}. {searchResponse.ServerError?.Error?.RootCause?.FirstOrDefault()?.Reason}");
                }
            }

            return commonLogsResult;
        }

        /// <summary>
        /// Получение Elastic запроса с фильтрацией
        /// </summary>
        /// <param name="storeLoadParams"></param>
        /// <param name="storeParams"></param>
        /// <returns></returns>
        private Func<BoolQueryDescriptor<ElasticLog>, IBoolQuery> GetElasticBoolQuery(List<FilterColumn> filter)
        {
            var eqFilters = filter.Where(f => f.@operator == "eq");
            var gteFilters = filter.Where(f => f.@operator == "gte");
            var lteFilters = filter.Where(f => f.@operator == "lte");
            var neFilters = filter.Where(f => f.@operator == "ne");
            var inFilters = filter.Where(f => f.@operator == "in");
            var likeFilters = filter.Where(f => f.@operator == "like");
            var mustQueries = new List<Func<QueryContainerDescriptor<ElasticLog>, QueryContainer>>();
            var mustNotQueries = new List<Func<QueryContainerDescriptor<ElasticLog>, QueryContainer>>();

            foreach (FilterColumn filterColumn in eqFilters)
            {
                switch (filterColumn.property.ToLower())
                {
                    case "objectcreatedate":
                        DateTime objectcreatedate;

                        if (!DateTime.TryParseExact(filterColumn.value, "dd.MM.yyyy", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out objectcreatedate))
                        {
                            throw new Exception($"Не верно указано значение для фильтра objectcreatedate = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> objectcreatedateGreaterQuery = (q) => q
                            .DateRange(d => d.Field(f => f.ObjectCreateDateString).GreaterThanOrEquals(objectcreatedate.Date.ToString("yyyy-MM-dd'T'HH:mm:ss.fff")));
                        mustQueries.Add(objectcreatedateGreaterQuery);
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> objectcreatedateLessQuery = (q) => q
                            .DateRange(d => d.Field(f => f.ObjectCreateDateString).LessThanOrEquals(objectcreatedate.AddDays(1).Date.ToString("yyyy-MM-dd'T'HH:mm:ss.fff")));
                        mustQueries.Add(objectcreatedateLessQuery);
                        break;
                    case "entityid":
                        long longentityid;

                        if (!long.TryParse(filterColumn.value, out longentityid))
                        {
                            throw new Exception($"Не верно указано значение для фильтра EntityId = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> entityidQuery = (q) => q.Term(f => f.EntityId, longentityid);
                        mustQueries.Add(entityidQuery);
                        break;
                    case "subdivisionid":
                        long longsubdivisionid;

                        if (!long.TryParse(filterColumn.value, out longsubdivisionid))
                        {
                            throw new Exception($"Не верно указано значение для фильтра SubdivisionId = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> subdivisionidQuery = (q) => q.Term(f => f.SubdivisionId, longsubdivisionid);
                        mustQueries.Add(subdivisionidQuery);
                        break;
                    case "entitytypecode":
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> entitytypecodeQuery = (q) => q.Term(f => f.EntityTypeCode, filterColumn.value);
                        mustQueries.Add(entitytypecodeQuery);
                        break;
                    case "ids":
                        List<string> ids = filterColumn.value.Split(',').ToList();
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> idsQuery = (q) => q.Ids(s => s.Values(ids));
                        mustQueries.Add(idsQuery);
                        break;
                }
            }

            foreach (FilterColumn filterColumn in gteFilters)
            {
                switch (filterColumn.property.ToLower())
                {
                    case "objectcreatedate":
                        DateTime objectcreatedate;

                        if (!DateTime.TryParseExact(filterColumn.value, "dd.MM.yyyy", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out objectcreatedate))
                        {
                            throw new Exception($"Не верно указано значение для фильтра objectcreatedate = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> objectcreatedateQuery = (q) => q
                            .DateRange(d => d.Field(f => f.ObjectCreateDateString).GreaterThanOrEquals(objectcreatedate.ToString("yyyy-MM-dd'T'HH:mm:ss.fff")));
                        mustQueries.Add(objectcreatedateQuery);
                        break;
                    case "entityid":
                        long longentityid;

                        if (!long.TryParse(filterColumn.value, out longentityid))
                        {
                            throw new Exception($"Не верно указано значение для фильтра EntityId = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> entityidQuery = (q) => q.LongRange(t => t.Field(f => f.EntityId).GreaterThanOrEquals(longentityid));
                        mustQueries.Add(entityidQuery);
                        break;
                }
            }

            foreach (FilterColumn filterColumn in lteFilters)
            {
                switch (filterColumn.property.ToLower())
                {
                    case "objectcreatedate":
                        DateTime objectcreatedate;

                        if (!DateTime.TryParseExact(filterColumn.value, "dd.MM.yyyy", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out objectcreatedate))
                        {
                            throw new Exception($"Не верно указано значение для фильтра objectcreatedate = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> objectcreatedateQuery = (q) => q
                            .DateRange(d => d.Field(f => f.ObjectCreateDateString).LessThanOrEquals(objectcreatedate.ToString("yyyy-MM-dd'T'HH:mm:ss.fff")));
                        mustQueries.Add(objectcreatedateQuery);
                        break;
                    case "entityid":
                        long longentityid;

                        if (!long.TryParse(filterColumn.value, out longentityid))
                        {
                            throw new Exception($"Не верно указано значение для фильтра EntityId = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> entityidQuery = (q) => q.LongRange(t => t.Field(f => f.EntityId).LessThanOrEquals(longentityid));
                        mustQueries.Add(entityidQuery);
                        break;
                }
            }

            foreach (FilterColumn filterColumn in neFilters)
            {
                switch (filterColumn.property.ToLower())
                {
                    case "objectcreatedate":
                        DateTime objectcreatedate;

                        if (!DateTime.TryParseExact(filterColumn.value, "dd.MM.yyyy", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out objectcreatedate))
                        {
                            throw new Exception($"Не верно указано значение для фильтра objectcreatedate = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> objectcreatedateGreaterQuery = (q) => q
                            .DateRange(d => d.Field(f => f.ObjectCreateDateString).LessThan(objectcreatedate.Date.ToString("yyyy-MM-dd'T'HH:mm:ss.fff")));
                        mustQueries.Add(objectcreatedateGreaterQuery);
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> objectcreatedateLessQuery = (q) => q
                            .DateRange(d => d.Field(f => f.ObjectCreateDateString).GreaterThan(objectcreatedate.AddDays(1).Date.ToString("yyyy-MM-dd'T'HH:mm:ss.fff")));
                        mustQueries.Add(objectcreatedateLessQuery);
                        break;
                    case "entityid":
                        long longentityid;

                        if (!long.TryParse(filterColumn.value, out longentityid))
                        {
                            throw new Exception($"Не верно указано значение для фильтра EntityId = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> entityidQuery = (q) => q.Term(f => f.EntityId, longentityid);
                        mustNotQueries.Add(entityidQuery);
                        break;
                    case "actiontype":
                        int intactiontype;

                        if (!int.TryParse(filterColumn.value, out intactiontype))
                        {
                            throw new Exception($"Не верно указано значение для фильтра actiontype = {filterColumn.value}");
                        }

                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> actiontypeQuery = (q) => q.Term(f => f.ActionType, intactiontype);
                        mustNotQueries.Add(actiontypeQuery);
                        break;
                }
            }

            foreach (FilterColumn filterColumn in inFilters)
            {
                if (filterColumn.property.ToLower() == "actiontype")
                {
                    long longactiontype;

                    if (!long.TryParse(filterColumn.value, out longactiontype))
                    {
                        throw new Exception($"Не верно указано значение для фильтра actiontype = {filterColumn.value}");
                    }

                    Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> actiontypeQuery = (q) => q.Term(f => f.ActionType, longactiontype);
                    mustQueries.Add(actiontypeQuery);
                }
            }

            foreach (FilterColumn filterColumn in likeFilters)
            {
                switch (filterColumn.property.ToLower())
                {
                    case "entitytype":
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> entitytypeQuery = (q) => q
                            .Match(q => q.Field(f => f.EntityType).Query(filterColumn.value).Fuzziness(Fuzziness.EditDistance(1)));
                        mustQueries.Add(entitytypeQuery);
                        break;
                    case "userlogin":
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> userloginQuery = (q) => q
                            .Match(q => q.Field(f => f.UserLogin).Query(filterColumn.value));
                        mustQueries.Add(userloginQuery);
                        break;
                    case "operator":
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> operatorQuery = (q) => q
                            .Match(q => q.Field(f => f.Operator).Query(filterColumn.value));
                        mustQueries.Add(operatorQuery);
                        break;
                    case "buisnescomment":
                        Func<QueryContainerDescriptor<ElasticLog>, QueryContainer> busnescommentQuery = (q) => q
                            .Match(q => q.Field(f => f.BuisnesComment).Query(filterColumn.value).Fuzziness(Fuzziness.EditDistance(1)));
                        mustQueries.Add(busnescommentQuery);
                        break;
                }
            }

            Func<BoolQueryDescriptor<ElasticLog>, IBoolQuery> boolQuery = null;

            if (mustQueries.Count > 0 && mustNotQueries.Count > 0)
            {
                boolQuery = (bq) => bq.Must(mustQueries).MustNot(mustNotQueries);
            }
            else if (mustQueries.Count > 0 && mustNotQueries.Count == 0)
            {
                boolQuery = (bq) => bq.Must(mustQueries);
            }
            else if (mustQueries.Count == 0 && mustNotQueries.Count > 0)
            {
                boolQuery = (bq) => bq.MustNot(mustNotQueries);
            }

            return boolQuery;
        }

        /// <summary>
        /// Получение Elastic запроса с фильтрацией в индекс log
        /// </summary>
        /// <param name="storeLoadParams"></param>
        /// <param name="storeParams"></param>
        /// <returns></returns>
        private Func<QueryContainerDescriptor<ElasticLogRawData>, QueryContainer> GetLogRawDataFilteredQuery(List<FilterColumn> filter)
        {
            Func<QueryContainerDescriptor<ElasticLogRawData>, QueryContainer> querySelector;
            var eqFilters = filter.Where(f => f.@operator == "eq");
            var likeFilters = filter.Where(f => f.@operator == "like");
            var mustQueries = new List<Func<QueryContainerDescriptor<ElasticLogRawData>, QueryContainer>>();

            foreach (FilterColumn filterColumn in eqFilters)
            {
                if (filterColumn.property == "LogId")
                {
                    Func<QueryContainerDescriptor<ElasticLogRawData>, QueryContainer> logIdQuery = (q) => q.Term(t => t.LogId, filterColumn.value);
                    mustQueries.Add(logIdQuery);
                }
                else
                {
                    Func<QueryContainerDescriptor<ElasticLogRawData>, QueryContainer> entitytypeQuery = (q) =>
                        q.Nested(n => n.Path(p => p.JsonData).Query(q1 => q1.Term(t => t.JsonData[filterColumn.property].Value.Name, filterColumn.value)));
                    mustQueries.Add(entitytypeQuery);
                }
            }

            foreach (FilterColumn filterColumn in likeFilters)
            {
                Func<QueryContainerDescriptor<ElasticLogRawData>, QueryContainer> entitytypeQuery = (q) => q.Nested(n => n
                    .Path(p => p.JsonData)
                    .Query(q1 => q1.Match(m => m.Field(f => f.JsonData[filterColumn.property].Value.Name).Query(filterColumn.value).Fuzziness(Fuzziness.EditDistance(1)))));
            }

            if (mustQueries.Count > 0)
            {
                Func<BoolQueryDescriptor<ElasticLogRawData>, IBoolQuery> boolQuery = (bq) => bq.Must(mustQueries);
                querySelector = (q) => q.Bool(boolQuery);
            }
            else
            {
                querySelector = (q) => q.MatchAll();
            }

            return querySelector;
        }

        /// <summary>
        /// Получение предыдущего обобщенного лога записи
        /// </summary>
        /// <param name="log"></param>
        /// <returns></returns>
        private async Task<ElasticLog> GetOldLogAsync(ElasticLog log)
        {
            var searchOldLogSearchDescripter = new SearchDescriptor<ElasticLog>()
                    .Index(LogIndexName)
                    .Size(1)
                    .Query(q => q
                        .Bool(b => b
                            .Must(m => m.Term(t => t.EntityId, log.EntityId)
                                && m.Term(t => t.EntityTypeCode, log.EntityTypeCode)
                                && m.DateRange(r => r.Field(f => f.ObjectCreateDateString).LessThan(log.ObjectCreateDate.ToString("yyyy-MM-dd'T'HH:mm:ss.fff"))))
                            .MustNot(mn => mn.Term("_id", log.Id))
                        )
                    )
                    .Sort(s => s.Field(f => f.ObjectCreateDateString, SortOrder.Descending));

            ElasticLog oldLog = await TryGetElasticDocumentAsync(searchOldLogSearchDescripter);

            return oldLog;
        }
        
        /// <summary>
        /// Получение сокращенной даты (yyyy-MM-dd) из поля elastic
        /// </summary>
        /// <param name="dateField">Поле elastic</param>
        /// <returns></returns>
        private string GetDateFromElasticLogField(ElasticLogField dateField)
        {
            DateTime date;
            string dateString = string.Empty;

            if (DateTime.TryParse(dateField?.Value?.Name, out date))
            {
                dateString = date.ToString("yyyy-MM-dd");
            }

            return dateString;
        }

        /// <summary>
        /// Получение измененных полей лога
        /// </summary>
        /// <param name="commoneLog">Общий лог</param>
        /// <param name="jsonData">Данные лога</param>
        /// <param name="exceptDtoFields">Исключить поля</param>
        private async Task<List<ContractLogMutation>> GetChangedFieldsAsync(ElasticLog commoneLog, Dictionary<string, ElasticLogField> jsonData, 
            List<string> exceptDtoFields, List<string> dateFields = null)
        {
            var changedFields = new List<ContractLogMutation>();
            ElasticLogRawData contractOldLogData = null;
            var checkedFields = new List<string>();

            if (commoneLog.ActionType == ElasticLogBuilder.ActionType.Edit)
            {
                ElasticLog contractCommonOldLog = await GetOldLogAsync(commoneLog);

                if (contractCommonOldLog != null)
                {
                    contractOldLogData = await GetLogRawDataAsync(contractCommonOldLog);

                    if (contractOldLogData == null)
                    {
                        _logger.LogError($"Не найден лог c данными для общего лога {contractCommonOldLog.Id}");
                    }
                }
            }

            #region Добавление измененных значений
            foreach (KeyValuePair<string, ElasticLogField> field in jsonData)
            {
                checkedFields.Add(field.Value.Name);

                if (exceptDtoFields.Contains(field.Key))
                {
                    continue;
                }

                string newValueString = field.Value.Value.Name;

                if (field.Key == "SettlementPaymentDate" && field.Value.Value.Name == "1000")
                {
                    newValueString = "последний день";
                }

                if (dateFields != null && dateFields.Contains(field.Key))
                {
                    newValueString = GetDateFromElasticLogField(field.Value);
                }

                var contractLogMutation = new ContractLogMutation { PropertyName = field.Value.Name };
                var elasticLogLinkValueResponseDto = new ElasticLogLinkValueResponseDto { Name = newValueString };

                if (!string.IsNullOrEmpty(field.Value.Value.LogId))
                {
                    elasticLogLinkValueResponseDto.Id = field.Value.Value.LogId;
                }

                if (commoneLog.ActionType == ElasticLogBuilder.ActionType.Delete)
                {
                    contractLogMutation.PropertyValueOld.Add(elasticLogLinkValueResponseDto);
                }
                else
                {
                    contractLogMutation.PropertyValueNew.Add(elasticLogLinkValueResponseDto);
                }

                if (commoneLog.ActionType == ElasticLogBuilder.ActionType.Edit && contractOldLogData != null)
                {
                    ElasticLogField oldDateField;

                    if (contractOldLogData.JsonData.TryGetValue(field.Key, out oldDateField))
                    {
                        string oldValueString = oldDateField.Value.Name;

                        if (field.Key == "SettlementPaymentDate" && oldDateField.Value.Name == "1000")
                        {
                            oldValueString = "последний день";
                        }

                        if (dateFields != null && dateFields.Contains(field.Key))
                        {
                            oldValueString = GetDateFromElasticLogField(oldDateField);
                        }

                        if (oldDateField.Name != field.Value.Name || oldDateField.Value.Id != field.Value.Value.Id || oldValueString != newValueString)
                        {
                            contractLogMutation.PropertyValueOld.Add(new ElasticLogLinkValueResponseDto { Id = field.Value.Value.LogId, Name = oldValueString });
                        }
                        else
                        {
                            continue;
                        }
                    }
                }

                changedFields.Add(contractLogMutation);
            }
            #endregion

            #region Добавление удаленных значений
            if (commoneLog.ActionType == ElasticLogBuilder.ActionType.Edit && contractOldLogData != null)
            {
                foreach (KeyValuePair<string, ElasticLogField> field in contractOldLogData.JsonData)
                {
                    if (!checkedFields.Contains(field.Value.Name))
                    {
                        string newValueString = field.Value.Value.Name;

                        if (field.Key == "SettlementPaymentDate" && field.Value.Value.Name == "1000")
                        {
                            newValueString = "последний день";
                        }

                        if (dateFields != null && dateFields.Contains(field.Key))
                        {
                            newValueString = GetDateFromElasticLogField(field.Value);
                        }

                        var contractLogMutation = new ContractLogMutation { PropertyName = field.Value.Name };
                        var elasticLogLinkValueResponseDto = new ElasticLogLinkValueResponseDto { Name = newValueString };

                        if (!string.IsNullOrEmpty(field.Value.Value.LogId))
                        {
                            elasticLogLinkValueResponseDto.Id = field.Value.Value.LogId;
                        }

                        contractLogMutation.PropertyValueOld.Add(elasticLogLinkValueResponseDto);
                        changedFields.Add(contractLogMutation);
                    }
                }
            }
            #endregion

            return changedFields;
        }

        /// <summary>
        /// Попытка получить данные лога по связи его ссылочного поля с id сущности
        /// </summary>
        /// <param name="indexName">Имя индекса</param>
        /// <param name="logLinkFieldName">Имя ссылочного поля</param>
        /// <param name="entityId">Ссылка на сущность</param>
        /// <param name="dataLogs">Логи данных</param>
        /// <returns></returns>
        private async Task<IReadOnlyCollection<ElasticLogRawData>> TryGetElasticLogRawDataByEntityIdLinkFieldAsync(string indexName, string logLinkFieldName, long entityId)
        {
            IReadOnlyCollection<ElasticLogRawData> dataLogs = null;
            SearchDescriptor<ElasticLogRawData> searchDescripter = new SearchDescriptor<ElasticLogRawData>().Index(indexName)
                        .Query(q => q.Nested(n => n.Path(p => p.JsonData).Query(q2 => q2.Term(t => t.JsonData[logLinkFieldName].Value.Id, entityId)))).Size(1000);
            var searchResult = await _client.SearchAsync<ElasticLogRawData>(s => searchDescripter);

            if (searchResult.IsValid)
            {
                dataLogs = searchResult.Documents;
            }

            return dataLogs;
        }

        /// <summary>
        /// Получение списка id общего лога связанных сущностей
        /// </summary>
        /// <param name="contractcontragentLogIndexName"></param>
        /// <param name="v"></param>
        /// <param name="entityid"></param>
        /// <returns></returns>
        private async Task<List<string>> GetRelatedEntityCommonLogIdsAsync(string dataLogIndexName, string refFieldName, long entityid)
        {
            //todo: учитывать storeParams.filter
            var commonLogIdList = new List<string>();
            IReadOnlyCollection<ElasticLogRawData> dataLogs = await TryGetElasticLogRawDataByEntityIdLinkFieldAsync(dataLogIndexName, refFieldName, entityid);

            if (dataLogs != null)
            {
                foreach (ElasticLogRawData logData in dataLogs)
                {
                    if (!commonLogIdList.Contains(logData.LogId))
                    {
                        commonLogIdList.Add(logData.LogId);
                    }
                }
            }

            return commonLogIdList;
        }

        /// <summary>
        /// Получение данных лога
        /// </summary>
        /// <param name="log"></param>
        /// <returns></returns>
        private async Task<ElasticLogRawData> GetLogRawDataAsync(ElasticLog log)
        {
            ElasticLogRawData logData;
            var logDataSearchDescripter = new SearchDescriptor<ElasticLogRawData>().Index(log.EntityTypeCode.ToLower()).Size(1).Query(q => q.Term(t => t.LogId, log.Id));
            logData = await TryGetElasticDocumentAsync(logDataSearchDescripter);
            return logData;
        }

        /// <summary>
        /// Проверяет недостающие поля и добавляет в историю
        /// </summary>
        /// <param name="logDataMutation"></param>
        /// <param name="logHistory"></param>
        private void AddMissingFields(List<RawLogMutation> logDataMutation, ref List<RawLogMutation> logHistory)
        {
            foreach (RawLogMutation logN in logDataMutation)
            {
                if (!logHistory.Contains(logN))
                {
                    logHistory.Add(new RawLogMutation
                    {
                        PropertyName = logN.PropertyName,
                        PropertyValueNew = logN.PropertyValueNew,
                        PropertyValueOld = logN.PropertyValueOld
                    });
                }
            }
        }        
        #endregion
    }
}
