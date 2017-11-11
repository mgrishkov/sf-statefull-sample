using Microsoft.ServiceFabric.Services.Remoting;
using System.Threading.Tasks;
using Sample.Service.UTILS.Logging.Abstractions.Data;

namespace Sample.Service.Logging.Abstractions.Interfaces
{
    public interface ILoggingService : IService
    {
        Task EnqueueAsync(MessageData message);
    }
}
