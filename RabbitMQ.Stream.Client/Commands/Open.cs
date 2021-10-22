using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct OpenRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.Open;
        public ushort Version => 1;
        public string VirtualHost { get; }

        public uint CorrelationId { get; }

        public OpenRequest(uint correlationId, string vhost)
        {
            CorrelationId = correlationId;
            VirtualHost = vhost;
        }

        public int SizeNeeded => 4 + WireFormatting.StringSize(VirtualHost);

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            return 4 + WireFormatting.WriteString(span.Slice(4), VirtualHost);
        }
    }

    public readonly struct OpenResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Open;
        public ushort Version => 1;

        private OpenResponse(uint correlationId, ushort responseCode, Dictionary<string, string> connectionProperties)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
            ConnectionProperties = connectionProperties;
        }

        public uint CorrelationId { get; }

        public ushort ResponseCode { get; }

        public Dictionary<string, string> ConnectionProperties { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out OpenResponse command)
        {
            // Tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out ushort tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out var correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out var responseCode);
            WireFormatting.ReadInt32(frame.Slice(10), out var numProps);
            int offset = 14;
            var props = new Dictionary<string, string>(numProps);
            for (var i = 0; i < numProps; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out var k);
                offset += WireFormatting.ReadString(frame.Slice(offset), out var v);
                props.Add(k, v);
            }
            command = new OpenResponse(correlation, responseCode, props);
            return offset;
        }
    }
}
