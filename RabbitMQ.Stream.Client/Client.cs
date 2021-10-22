
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace RabbitMQ.Stream.Client
{
    public record ClientParameters
    {
        public Dictionary<string, string> Properties { get; } =
            new Dictionary<string, string>
            {
                {"product", "RabbitMQ Stream"},
                {"version", "0.1.0"},
                {"platform", ".NET"},
                {"copyright", "Copyright (c) 2020-2021 VMware, Inc. or its affiliates."},
                {"information", "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"}
            };
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public EndPoint Endpoint { get; set; } = new IPEndPoint(IPAddress.Loopback, 5552);
        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };
        public Action<Exception> UnhandledExceptionHandler { get; set; } = _ => { };
    }

    public readonly struct OutgoingMsg
    {
        public byte PublisherId { get; }
        public ulong PublishingId { get; }
        public Message Data { get; }

        public OutgoingMsg(byte publisherId, ulong publishingId, Message data)
        {
            PublisherId = publisherId;
            PublishingId = publishingId;
            Data = data;
        }
    }
    
    public class Client
    {
        private uint correlationId = 0; // allow for some pre-amble
        private byte nextPublisherId = 0;
        private readonly ClientParameters parameters;
        private Connection connection;
        private readonly IDictionary<byte, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>)> publishers =
            new ConcurrentDictionary<byte, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>)>();
        private readonly ConcurrentDictionary<uint, IValueTaskSource> requests = new();
        private TaskCompletionSource<Tune> tuneReceived = new TaskCompletionSource<Tune>(TaskCreationOptions.RunContinuationsAsynchronously);

        private byte nextSubscriptionId;
        private readonly IDictionary<byte, Func<Deliver, Task>> consumers = new ConcurrentDictionary<byte, Func<Deliver, Task>>();

        private object closeResponse;
        public int PublishCommandsSent { get; private set; }

        public int MessagesSent { get; private set; }

        public int ConfirmFrames { get; private set; }
        public int IncomingFrames => connection.NumFrames;
        //public int IncomingChannelCount => this.incoming.Reader.Count;

        public bool IsClosed => closeResponse != null;

        private Client(ClientParameters parameters)
        {
            //this.connection = connection;
            this.parameters = parameters;
            
            // connection.CommandCallback = async (command) =>
            // {
            //     await HandleIncoming(command, parameters.MetadataHandler);
            // };
            //authenticate
            //var ts = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.ExecuteSynchronously);
        }

        // channels and message publish aggregation
        public static async Task<Client> Create(ClientParameters parameters)
        {
            var client = new Client(parameters);
            client.connection = await Connection.Create(parameters.Endpoint, client.HandleIncoming);
            
            // exchange properties
            var peerPropertiesResponse = await client.Request<PeerPropertiesRequest, PeerPropertiesResponse>(corr => new PeerPropertiesRequest(corr, parameters.Properties));
            foreach (var (k, v) in peerPropertiesResponse.Properties)
                Console.WriteLine($"server Props {k} {v}");

            //auth
            var saslHandshakeResponse = await client.Request<SaslHandshakeRequest, SaslHandshakeResponse>(corr => new SaslHandshakeRequest(corr));
            foreach (var m in saslHandshakeResponse.Mechanisms)
                Console.WriteLine($"sasl mechanism: {m}");

            var saslData = Encoding.UTF8.GetBytes($"\0{parameters.UserName}\0{parameters.Password}");
            var authResponse = await client.Request<SaslAuthenticateRequest, SaslAuthenticateResponse>(corr => new SaslAuthenticateRequest(corr, "PLAIN", saslData));
            Console.WriteLine($"auth: {authResponse.ResponseCode} {authResponse.Data}");

            //tune
            var tune = await client.tuneReceived.Task;
            await client.Publish(new Tune(0, 0));
            
            // open 
            var open = await client.Request<OpenRequest, OpenResponse>(corr => new OpenRequest(corr, "/"));
            Console.WriteLine($"open: {open.ResponseCode} {open.ConnectionProperties.Count}");
            foreach (var (k, v) in open.ConnectionProperties)
                Console.WriteLine($"open prop: {k} {v}");
            
            client.correlationId = 100;
            return client;
        }

        public async ValueTask<bool> Publish(Publish publishMsg)
        {
            var publishTask = Publish<Publish>(publishMsg);
            if(!publishTask.IsCompletedSuccessfully)
            {
                await publishTask.ConfigureAwait(false);
            }

            PublishCommandsSent += 1;
            MessagesSent += publishMsg.Messages.Count;
            return publishTask.Result;
        }

        public ValueTask<bool> Publish<T>(T msg) where T : struct, IClientCommand
        {
            return connection.Write(msg);
        }

        public async Task<(byte, DeclarePublisherResponse)> DeclarePublisher(string publisherRef,
            string stream,
            Action<ReadOnlyMemory<ulong>> confirmCallback,
            Action<(ulong, ResponseCode)[]> errorCallback)
        {
            var publisherId = nextPublisherId++;
            publishers.Add(publisherId, (confirmCallback, errorCallback));
            return (publisherId, await Request<DeclarePublisherRequest, DeclarePublisherResponse>(corr =>
               new DeclarePublisherRequest(corr, publisherId, publisherRef, stream)));
        }
        
        public async Task<DeletePublisherResponse> DeletePublisher(byte publisherId)
        {
            var result = await Request<DeletePublisherRequest, DeletePublisherResponse>(corr => new DeletePublisherRequest(corr, publisherId));
            publishers.Remove(publisherId);
            return result;
        }


        public async Task<(byte, SubscribeResponse)> Subscribe(string stream, IOffsetType offsetType, ushort initialCredit,
            Dictionary<string, string> properties, Func<Deliver, Task> deliverHandler)
        {
            var subscriptionId = nextSubscriptionId++;
            consumers.Add(subscriptionId, deliverHandler);
            return (subscriptionId,
                await Request<SubscribeRequest, SubscribeResponse>(corr =>
                   new SubscribeRequest(corr, subscriptionId, stream, offsetType, initialCredit, properties)));
        }
        
        public async Task<UnsubscribeResponse> Unsubscribe(byte subscriptionId)
        {
            var result = await Request<UnsubscribeRequest, UnsubscribeResponse>(corr => new UnsubscribeRequest(corr, subscriptionId));
            // remove consumer after RPC returns, this should avoid uncorrelated data being sent
            consumers.Remove(subscriptionId);
            return result;
        }

        private async ValueTask<TOut> Request<TIn, TOut>(Func<uint, TIn> request, int timeout = 10000) where TIn : struct, ICommandRequest where TOut : struct, ICommandResponse
        {
            var corr = NextCorrelationId();
            var tcs = PooledTaskSource<TOut>.Rent();
            requests.TryAdd(corr, tcs);
            await Publish(request(corr)).ConfigureAwait(false);
            using (CancellationTokenSource cts = new CancellationTokenSource(timeout))
            {
                using (cts.Token.Register(valueTaskSource => ((ManualResetValueTaskSource<TOut>)valueTaskSource).SetException(new TimeoutException()), tcs))
                {
                    var valueTask = new ValueTask<TOut>(tcs, tcs.Version);
                    var result = await valueTask;
                    PooledTaskSource<TOut>.Return(tcs);
                    return result;
                }
            }
        }

        private uint NextCorrelationId()
        {
            return Interlocked.Increment(ref correlationId);
        }

        private Task HandleIncoming(ReadOnlyMemory<byte> frameMemory)
        {
            WireFormatting.ReadUInt32(frameMemory.Span, out uint header);
            bool isResponse = ((header >> 16) & 0b1000_0000_0000_0000) > 0;
            CommandKey command = (CommandKey)((header >> 16) & 0b0111_1111_1111_1111);
            return isResponse ? HandleResponse(command, ref frameMemory) : HandleCommand(command, ref frameMemory);
        }

        private Task HandleCommand(CommandKey command, ref ReadOnlyMemory<byte> frameMemory)
        {
            if (command == CommandKey.Deliver)
            {
                Deliver.Read(ref frameMemory, out Deliver deliver);
                var deliverHandler = consumers[deliver.SubscriptionId];
                return deliverHandler(deliver);
            }
            else
            {
                var frame = new ReadOnlySequence<byte>(frameMemory);
                switch (command)
                {
                    case CommandKey.PublishConfirm:
                        PublishConfirm.Read(frame, out PublishConfirm confirm);
                        ConfirmFrames += 1;
                        var (confirmCallback, _) = publishers[confirm.PublisherId];
                        confirmCallback(confirm.PublishingIds);
                        if (MemoryMarshal.TryGetArray(confirm.PublishingIds, out ArraySegment<ulong> confirmSegment))
                        {
                            ArrayPool<ulong>.Shared.Return(confirmSegment.Array);
                        }
                        break;
                    case CommandKey.PublishError:
                        PublishError.Read(frame, out PublishError error);
                        var (_, errorCallback) = publishers[error.PublisherId];
                        errorCallback(error.PublishingErrors);
                        break;
                    case CommandKey.MetadataUpdate:
                        MetaDataUpdate.Read(frame, out MetaDataUpdate metaDataUpdate);
                        parameters.MetadataHandler(metaDataUpdate);
                        break;
                    case CommandKey.Tune:
                        Tune.Read(frame, out Tune tuneResponse);
                        tuneReceived.SetResult(tuneResponse);
                        break;
                    default:
                        throw new ArgumentException($"Unknown or unexpected command: {command}", nameof(command));
                }
            }

            return Task.CompletedTask;
        }

        private Task HandleResponse(CommandKey commandKey, ref ReadOnlyMemory<byte> frameMemory)
        {
            var frame = new ReadOnlySequence<byte>(frameMemory);
            switch (commandKey)
            {
                case CommandKey.DeclarePublisher:
                    DeclarePublisherResponse.Read(frame, out var declarePublisherResponse);
                    HandleCorrelatedResponse(declarePublisherResponse);
                    break;
                case CommandKey.QueryPublisherSequence:
                    QueryPublisherSequenceResponse.Read(frame, out var queryPublisherResponse);
                    HandleCorrelatedResponse(queryPublisherResponse);
                    break;
                case CommandKey.DeletePublisher:
                    DeletePublisherResponse.Read(frame, out var deletePublisherResponse);
                    HandleCorrelatedResponse(deletePublisherResponse);
                    break;
                case CommandKey.Subscribe:
                    SubscribeResponse.Read(frame, out var subscribeResponse);
                    HandleCorrelatedResponse(subscribeResponse);
                    break;
                case CommandKey.QueryOffset:
                    QueryOffsetResponse.Read(frame, out var queryOffsetResponse);
                    HandleCorrelatedResponse(queryOffsetResponse);
                    break;
                case CommandKey.Unsubscribe:
                    UnsubscribeResponse.Read(frame, out var unsubscribeResponse);
                    HandleCorrelatedResponse(unsubscribeResponse);
                    break;
                case CommandKey.Create:
                    CreateResponse.Read(frame, out var createResponse);
                    HandleCorrelatedResponse(createResponse);
                    break;
                case CommandKey.Delete:
                    DeleteResponse.Read(frame, out var deleteResponse);
                    HandleCorrelatedResponse(deleteResponse);
                    break;
                case CommandKey.Metadata:
                    MetaDataResponse.Read(frame, out var metaDataResponse);
                    HandleCorrelatedResponse(metaDataResponse);
                    break;
                case CommandKey.PeerProperties:
                    PeerPropertiesResponse.Read(frame, out var peerPropertiesResponse);
                    HandleCorrelatedResponse(peerPropertiesResponse);
                    break;
                case CommandKey.SaslHandshake:
                    SaslHandshakeResponse.Read(frame, out var saslHandshakeResponse);
                    HandleCorrelatedResponse(saslHandshakeResponse);
                    break;
                case CommandKey.SaslAuthenticate:
                    SaslAuthenticateResponse.Read(frame, out var saslAuthenticateResponse);
                    HandleCorrelatedResponse(saslAuthenticateResponse);
                    break;
                case CommandKey.Open:
                    OpenResponse.Read(frame, out var openResponse);
                    HandleCorrelatedResponse(openResponse);
                    break;
                case CommandKey.Close:
                    CloseResponse.Read(frame, out var closeResponse);
                    HandleCorrelatedResponse(closeResponse);
                    break;
                default:
                    if (MemoryMarshal.TryGetArray(frame.First, out ArraySegment<byte> segment))
                    {
                        ArrayPool<byte>.Shared.Return(segment.Array);
                    }

                    throw new ArgumentException($"Unknown or unexpected command: {commandKey}", nameof(commandKey));
            }

            return Task.CompletedTask;
        }

        private void HandleCorrelatedResponse<T>(T command) where T : struct, ICommandResponse
        {
            if (command.CorrelationId == uint.MaxValue)
            {
                throw new Exception($"unhandled incoming command {command.GetType()}");
            }

            if (requests.TryRemove(command.CorrelationId, out var tsc))
            {
                ((ManualResetValueTaskSource<T>)tsc).SetResult(command);
            }
        }

        public async Task<CloseResponse> Close(string reason)
        {
            if (closeResponse != null)
            {
                return (CloseResponse) closeResponse;
            }

            var result = await Request<CloseRequest, CloseResponse>(corr => new CloseRequest(corr, reason));
            closeResponse = result;
            connection.Dispose();

            return result;
        }

        public async ValueTask<QueryPublisherSequenceResponse> QueryPublisherSequence(string publisherRef, string stream)
        {
            return await Request<QueryPublisherSequenceRequest, QueryPublisherSequenceResponse>(corr => new QueryPublisherSequenceRequest(corr, publisherRef, stream));
        }

        public async ValueTask<bool> StoreOffset(string reference, string stream, ulong offsetValue)
        {
            return await Publish(new StoreOffset(stream, reference, offsetValue));
        }

        public async ValueTask<MetaDataResponse> QueryMetadata(string[] streams)
        {
            return await Request<MetaDataQuery, MetaDataResponse>(corr => new MetaDataQuery(corr, streams.ToList()));
        }

        public async ValueTask<QueryOffsetResponse> QueryOffset(string reference, string stream)
        {
            return await Request<QueryOffsetRequest, QueryOffsetResponse>(corr => new QueryOffsetRequest(stream, corr, reference));
        }

        public async ValueTask<CreateResponse> CreateStream(string stream, Dictionary<string, string> args)
        {
            return await Request<CreateRequest, CreateResponse>(corr => new CreateRequest(corr, stream, args));
        }

        public async ValueTask<DeleteResponse> DeleteStream(string stream)
        {
            return await Request<DeleteRequest, DeleteResponse>(corr => new DeleteRequest(corr, stream));
        }

        public async ValueTask<bool> Credit(byte subscriptionId, ushort credit)
        {
            return await Publish(new Credit(subscriptionId, credit));
        }
    }



    public static class PooledTaskSource<T>
    {
        private static ConcurrentStack<ManualResetValueTaskSource<T>> stack = new ConcurrentStack<ManualResetValueTaskSource<T>>();
        public static ManualResetValueTaskSource<T> Rent()
        {
            if(stack.TryPop(out ManualResetValueTaskSource<T> task))
            {
                return task;
            }
            else
            {
                return new ManualResetValueTaskSource<T>() { RunContinuationsAsynchronously = true };
            }
        }

        public static void Return(ManualResetValueTaskSource<T> task)
        {
            task.Reset();
            stack.Push(task);
        }
    }

    public sealed class ManualResetValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<T> _logic; // mutable struct; do not make this readonly

        public bool RunContinuationsAsynchronously
        {
            get => _logic.RunContinuationsAsynchronously;
            set => _logic.RunContinuationsAsynchronously = value;
        }

        public short Version => _logic.Version;
        public void Reset() => _logic.Reset();
        public void SetResult(T result) => _logic.SetResult(result);
        public void SetException(Exception error) => _logic.SetException(error);

        void IValueTaskSource.GetResult(short token) => _logic.GetResult(token);
        T IValueTaskSource<T>.GetResult(short token) => _logic.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _logic.GetStatus(token);
        ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => _logic.GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _logic.OnCompleted(continuation, state, token, flags);
        void IValueTaskSource<T>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _logic.OnCompleted(continuation, state, token, flags);
    }
}
