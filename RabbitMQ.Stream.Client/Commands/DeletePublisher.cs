using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeletePublisherRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.DeletePublisher;
        public ushort Version => 1;
        public byte PublisherId { get; }
        public uint CorrelationId { get; }

        public DeletePublisherRequest(uint correlationId, byte publisherId)
        {
            CorrelationId = correlationId;
            PublisherId = publisherId;
        }

        public int SizeNeeded => 5;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            WireFormatting.WriteByte(span.Slice(4), PublisherId);
            return 5;
        }
    }

    public readonly struct DeletePublisherResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.DeletePublisher;
        public ushort Version => 1;
        public uint CorrelationId { get; }
        public ushort ResponseCode { get; }
        
        public DeletePublisherResponse(uint correlationId, ushort responseCode)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
        }

        internal static int Read(ReadOnlySequence<byte> frame, out DeletePublisherResponse command)
        {
            // Tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out ushort tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            command = new DeletePublisherResponse(correlation, responseCode);
            return 10;
        }
    }
}
