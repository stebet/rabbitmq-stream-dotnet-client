using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeclarePublisherRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.DeclarePublisher;
        public ushort Version => 1;
        public byte PublisherId { get; }
        public string PublisherRef { get; }
        public string Stream { get; }
        public uint CorrelationId { get; }

        public DeclarePublisherRequest(uint correlationId, byte publisherId, string publisherRef, string stream)
        {
            CorrelationId = correlationId;
            PublisherId = publisherId;
            PublisherRef = publisherRef;
            Stream = stream;
        }

        public int SizeNeeded => 4 + 1 + WireFormatting.StringSize(PublisherRef) + WireFormatting.StringSize(Stream);

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            WireFormatting.WriteByte(span.Slice(4), PublisherId);
            int offset = 5 + WireFormatting.WriteString(span.Slice(5), PublisherRef);
            offset += WireFormatting.WriteString(span.Slice(offset), Stream);
            return offset;
        }
    }

    public readonly struct DeclarePublisherResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.DeclarePublisher;
        public ushort Version => 1;
        public ushort ResponseCode { get; }
        public uint CorrelationId { get; }

        public DeclarePublisherResponse(uint correlationId, ushort responseCode)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
        }

        internal static int Read(ReadOnlySequence<byte> frame, out DeclarePublisherResponse command)
        {
            // tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out ushort tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            command = new DeclarePublisherResponse(correlation, responseCode);
            return 10;
        }
    }
}
