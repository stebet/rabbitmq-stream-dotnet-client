using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryOffsetRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.QueryOffset;
        public ushort Version => 1;
        public string Stream { get; }
        public string Reference { get; }
        public uint CorrelationId { get; }

        public QueryOffsetRequest(string stream, uint correlationId, string reference)
        {
            CorrelationId = correlationId;
            Reference = reference;
            Stream = stream;
        }

        public int SizeNeeded => 4 + WireFormatting.StringSize(Reference) + WireFormatting.StringSize(Stream);

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            int offset = 4 + WireFormatting.WriteString(span.Slice(4), Reference);
            return offset + WireFormatting.WriteString(span.Slice(offset), Stream);
        }
    }

    public readonly struct QueryOffsetResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.QueryOffset;
        public ushort Version => 1;

        public QueryOffsetResponse(uint correlationId, ushort responseCode, ulong offsetValue)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
            Offset = offsetValue;
        }

        public uint CorrelationId { get; }
        public ushort ResponseCode { get; }
        public ulong Offset { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out QueryOffsetResponse command)
        {
            // Key and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out ushort key);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            WireFormatting.ReadUInt64(frame.Slice(10), out ulong offsetValue);
            command = new QueryOffsetResponse(correlation, responseCode, offsetValue);
            return 18;
        }
    }
}