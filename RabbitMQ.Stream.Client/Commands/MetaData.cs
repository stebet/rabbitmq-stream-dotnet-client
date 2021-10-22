using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    public readonly struct MetaDataQuery : ICommandRequest
    {
        public CommandKey Key => CommandKey.Metadata;
        public ushort Version => 1;
        public List<string> Streams { get; }
        public uint CorrelationId { get; }

        public MetaDataQuery(uint correlationId, List<string> streams)
        {
            CorrelationId = correlationId;
            Streams = streams;
        }

        public int SizeNeeded
        {
            get
            {
                int size = 8;
                foreach (var s in Streams)
                {
                    // TODO: unnecessary conversion work here to work out the correct size of the frame
                    size += WireFormatting.StringSize(s); //
                }

                return size;
            }
        }
        
        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            // map
            WireFormatting.WriteInt32(span.Slice(4), Streams.Count);
            int offset = 8;
            foreach(var s in Streams)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), s);
            }

            return offset;
        }
    }

    public readonly struct Broker
    {
        public string Host { get; }

        public uint Port { get; }

        public Broker(string host, uint port)
        {
            Host = host;
            Port = port;
        }
    }
    
    public readonly struct StreamInfo
    {
        public string Stream { get; }
        public ushort Code { get; }
        public Broker Leader { get; }
        public IList<Broker> Replicas { get; }

        public StreamInfo(string stream, ushort code, Broker leader, IList<Broker> replicas)
        {
            Stream = stream;
            Code = code;
            Leader = leader;
            Replicas = replicas;
        }
    }
    
    public readonly struct MetaDataResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Metadata;
        public ushort Version => 1;

        public MetaDataResponse(uint correlationId, Dictionary<string, StreamInfo> streamInfos)
        {
            StreamInfos = streamInfos;
            CorrelationId = correlationId;
        }

        public uint CorrelationId { get; }

        public Dictionary<string, StreamInfo> StreamInfos { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out MetaDataResponse command)
        {
            // Tag and version are not used yet
            // WireFormatting.ReadUInt16(frame, out var tag);
            // WireFormatting.ReadUInt16(frame.Slice(2), out var version);
            WireFormatting.ReadUInt32(frame.Slice(4), out var correlation);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            WireFormatting.ReadUInt32(frame.Slice(8), out var numBrokers);
            var brokers = new Dictionary<ushort, Broker>((int)numBrokers);
            int offset = 12;
            for (int i = 0; i < numBrokers; i++)
            {
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var brokerRef);
                offset += WireFormatting.ReadString(frame.Slice(offset), out var host);
                offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var port);
                brokers.Add(brokerRef, new Broker(host, port));
            }
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numStreams);
            var streamInfos = new Dictionary<string, StreamInfo>((int)numStreams);
            for (int i = 0; i < numStreams; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out var stream);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var code);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var leaderRef);
                offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numReplicas);
                var replicas = new List<Broker>((int)numReplicas);
                for (var j = 0; j < numReplicas; j++)
                {
                    offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort replicaRef);
                    replicas.Add(brokers[replicaRef]);
                }

                var leader = brokers[leaderRef];
                streamInfos.Add(stream, new StreamInfo(stream, code, leader, replicas));
            }

            command = new MetaDataResponse(correlation, streamInfos);
            return offset;
        }
    }
    
    public readonly struct MetaDataUpdate : ICommandResponse
    {
        public CommandKey Key => CommandKey.MetadataUpdate;
        public ushort Version => 1;

        public uint CorrelationId => uint.MaxValue;

        public MetaDataUpdate(string stream, ResponseCode code)
        {
            Stream = stream;
            Code = code;
        }

        public ResponseCode Code { get; }

        public string Stream { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out MetaDataUpdate command)
        {
            // Tag and version are not used yet.
            // WireFormatting.ReadUInt16(frame, out var tag);
            // WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadUInt16(frame.Slice(4), out var code);
            int offset = 6 + WireFormatting.ReadString(frame.Slice(6), out var stream);
            command = new MetaDataUpdate(stream, (ResponseCode)code);
            return offset;
        }
    }
}
