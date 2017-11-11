﻿using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Sample.Service.Logging.Abstractions.Interfaces;
using Sample.Service.UTILS.Logging.Abstractions.Constants;
using Sample.Service.UTILS.Logging.Abstractions.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Sample.Service.UTILS.Logging.Abstractions.Enums;

namespace Sample.Service.UTILS.Logging
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    public sealed class LoggingService : StatefulService, ILoggingService
    {

        private const int MAX_QUEUE_LENGTH        = 10000;
        private const int FAIL_ATTEMPTS_THRESHOLD = 2;

        private readonly SemaphoreSlim _signal;

        private readonly Queue<TimeSpan> _delays;

        private int _failProcessingAttemptsCount;
        private CancellationToken _cancellationToken;
        private IReliableConcurrentQueue<MessageData> _queue;

        public State ProcessingState { get; private set; }
        public DateTime NextRun      { get; private set; }
        public long CurrentQueueLength => _queue?.Count ?? 0;
        public long MaxQueueLength => MAX_QUEUE_LENGTH;


        public LoggingService(StatefulServiceContext context): base(context)
        {
            ProcessingState = State.Warming;
            _signal = new SemaphoreSlim(0);
            _delays = new Queue<TimeSpan>();

            _delays.Enqueue(TimeSpan.FromMinutes( 1));
            _delays.Enqueue(TimeSpan.FromMinutes( 5));
            _delays.Enqueue(TimeSpan.FromMinutes(10));
            _delays.Enqueue(TimeSpan.FromMinutes(15));
        }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;

            _queue = await StateManager.GetOrAddAsync<IReliableConcurrentQueue<MessageData>>(ServiceConstants.APPLICATION_LOG_QUEUE);

            while (true)
            {
                //Waiting for signal resolution
                await _signal.WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                //Waiting for better condition to messages processig
                await WaitAsync();

                cancellationToken.ThrowIfCancellationRequested();

                await ProcessAsync();
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

            //We have to remove old messages in case of queue overflow condition
            if (_queue.Count > MAX_QUEUE_LENGTH)
                await TrimAsync();


            _signal.Release();
        }

        private Task WaitAsync()
        {
            if (_failProcessingAttemptsCount < FAIL_ATTEMPTS_THRESHOLD)
                return Task.CompletedTask;

            var delay = _delays.Dequeue();
            _failProcessingAttemptsCount = 0;

            ProcessingState = State.Freeze;

            NextRun = DateTime.UtcNow.Add(delay);
            ServiceEventSource.Current.ServiceMessage(this.Context, "Processing has been freezed due to all attempts expended. Next round at {0}.", NextRun);

            //return delay back to the queue
            _delays.Enqueue(delay);

            return Task.Delay(delay, _cancellationToken);
        }

        private async Task ProcessAsync()
        {
            if (_queue.Count == 0)
                return;

            ProcessingState = State.Processing;

            using (var tx = StateManager.CreateTransaction())
            {
                var message = await _queue.TryDequeueAsync(tx, _cancellationToken);

                if (!message.HasValue)
                    tx.Abort();

                if (await TryProcessMessageAsync(message.Value))
                    await tx.CommitAsync();
                else
                    tx.Abort();
            }

            //Synchronize message count in queue and semaphore
            var delta = Convert.ToInt32(_queue.Count - _signal.CurrentCount);

            if (delta > 0)
                _signal.Release(delta);
        }

        private async Task TrimAsync(int trimPercentage = 1)
        {
            ProcessingState = State.Cleansing;

            var toBeRemoved = MAX_QUEUE_LENGTH * trimPercentage / 100;

            using (var tx = StateManager.CreateTransaction())
            {
                for(var i = 0; i < toBeRemoved; i++)
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
                //TODO Logger implementation must be here 
                await Task.Factory.StartNew(() =>
                {
                    ServiceEventSource.Current.ServiceMessage(this.Context, Newtonsoft.Json.JsonConvert.SerializeObject(message));
                }, _cancellationToken);

                _failProcessingAttemptsCount = 0;

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
