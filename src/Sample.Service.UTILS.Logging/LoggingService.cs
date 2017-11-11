using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Sample.Service.Logging.Abstractions.Interfaces;
using Sample.Service.UTILS.Logging.Abstractions.Constants;
using Sample.Service.UTILS.Logging.Abstractions.Data;
using Sample.Service.UTILS.Logging.Enums;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace Sample.Service.UTILS.Logging
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    public sealed class LoggingService : StatefulService, ILoggingService
    {

        private const int QUEUE_LENGTH            = 10000;
        private const int FAIL_ATTEMPTS_THRESHOLD = 2;

        private readonly TimeSpan _defaultDelay;
        private readonly TimeSpan _delayAfterProcessingFails;

        private int _failProcessingAttemptsCount;
        private CancellationToken _cancellationToken;
        private IReliableConcurrentQueue<MessageData> _queue;

        public State ProcessingState { get; private set; }
        public DateTime NextRun      { get; private set; }


        public LoggingService(StatefulServiceContext context): base(context)
        {
            ProcessingState = State.Warming;

            _defaultDelay              = TimeSpan.FromSeconds(1);
            _delayAfterProcessingFails = TimeSpan.FromSeconds(60);
        }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;

            _queue = await StateManager.GetOrAddAsync<IReliableConcurrentQueue<MessageData>>(ServiceConstants.APPLICATION_LOG_QUEUE);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if(_queue.Count > 0)
                    await ProcessAsync();

                await WaitAsync();
            }
        }

        public async Task EnqueueAsync(MessageData message)
        {
            if(message == null)
                throw new ArgumentNullException(nameof(message));

            using (var tran = StateManager.CreateTransaction())
            {               
                await _queue.EnqueueAsync(tran, message, _cancellationToken);
                
                await tran.CommitAsync();
            }

            await CleanseAsync();
        }

        private async Task WaitAsync()
        {
            ProcessingState = State.Idle;

            var delay = _defaultDelay;
            if (_failProcessingAttemptsCount >= FAIL_ATTEMPTS_THRESHOLD)
            {
                delay = _delayAfterProcessingFails;
                _failProcessingAttemptsCount = 0;

                ProcessingState = State.Freeze;
            }
            NextRun = DateTime.UtcNow.Add(delay);

            if(ProcessingState == State.Freeze)
                ServiceEventSource.Current.ServiceMessage(this.Context, "Processing has been freezed due to all attempts has been expended. Next round at {0}.", NextRun);

            await Task.Delay(delay, _cancellationToken);
        }

        private async Task ProcessAsync()
        {
            ProcessingState = State.Processing;

            using (var tx = StateManager.CreateTransaction())
            {
                var message = await _queue.TryDequeueAsync(tx, _cancellationToken);

                if (!message.HasValue)
                    await tx.CommitAsync();

                if (await TryProcessMessageAsync(message.Value))
                    await tx.CommitAsync();
                else
                    tx.Abort();
            }
        }

        private async Task CleanseAsync()
        {
            if (_queue.Count <= QUEUE_LENGTH)
                return;

            ProcessingState = State.Cleansing;
            using (var tx = StateManager.CreateTransaction())
            {
                for(var i = 0; i < QUEUE_LENGTH / 100; i++)
                {
                    var message = await _queue.TryDequeueAsync(tx, _cancellationToken);

                    if (message.HasValue && message.Value != null)
                        ServiceEventSource.Current.ServiceMessage(this.Context, "Message has been cleanced due to queue oveflow. Message: {0}", Newtonsoft.Json.JsonConvert.SerializeObject(message.Value));
                }

                await tx.CommitAsync();
            }
        }

        private async Task<bool> TryProcessMessageAsync(MessageData message)
        {
            if(message == null)
                return await Task.FromResult(true);

            try
            {
                _failProcessingAttemptsCount = 0;

                //TODO Logger implementation must be here 
                await Task.Factory.StartNew(() =>
                {
                    ServiceEventSource.Current.ServiceMessage(this.Context, Newtonsoft.Json.JsonConvert.SerializeObject(message));
                }, _cancellationToken);


                return await Task.FromResult(true);
            }
            catch (Exception ex)
            {
                _failProcessingAttemptsCount++;

                ServiceEventSource.Current.ServiceMessage(this.Context, "Unable to process message due to [{0}]. Attempt {1} of {2}.", ex.Message, _failProcessingAttemptsCount, FAIL_ATTEMPTS_THRESHOLD);

                return await Task.FromResult(false);
            }
        }
    }
}
