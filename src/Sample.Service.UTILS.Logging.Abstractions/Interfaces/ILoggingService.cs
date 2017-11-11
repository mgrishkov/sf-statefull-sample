using System;
using Microsoft.ServiceFabric.Services.Remoting;
using System.Threading.Tasks;
using Sample.Service.UTILS.Logging.Abstractions.Data;
using Sample.Service.UTILS.Logging.Abstractions.Enums;

namespace Sample.Service.Logging.Abstractions.Interfaces
{
    public interface ILoggingService : IService
    {
        long MaxQueueLength { get; }
        long CurrentQueueLength { get; }
        State ProcessingState { get; }
        DateTime NextRun { get; }

        Task EnqueueAsync(MessageData message);
    }
}
