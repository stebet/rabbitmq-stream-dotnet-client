using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryPublisherSequenceRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.QueryPublisherSequence;
        public ushort Version => 1;
        public string PublisherRef { get; }
        public string Stream { get; }

        public uint CorrelationId { get; }

        public QueryPublisherSequenceRequest(uint correlationId, string publisherRef, string stream)
        {
            CorrelationId = correlationId;
            PublisherRef = publisherRef;
            Stream = stream;
        }
        public int SizeNeeded => 4 + WireFormatting.StringSize(PublisherRef) + WireFormatting.StringSize(Stream);

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            int offset = 4 + WireFormatting.WriteString(span.Slice(4), PublisherRef);
            return offset + WireFormatting.WriteString(span.Slice(offset), Stream);
        }
    }

    public readonly struct QueryPublisherSequenceResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.QueryPublisherSequence;
        public ushort Version => 1;

        public QueryPublisherSequenceResponse(uint correlationId, ushort responseCode, ulong sequence)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
            Sequence = sequence;
        }

        public uint CorrelationId { get; }

        public ushort ResponseCode { get; }

        public ulong Sequence { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out QueryPublisherSequenceResponse command)
        {
            // Tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out ushort tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            WireFormatting.ReadUInt64(frame.Slice(10), out ulong sequence);
            command = new QueryPublisherSequenceResponse(correlation, responseCode, sequence);
            return 18;
        }
    }
}
