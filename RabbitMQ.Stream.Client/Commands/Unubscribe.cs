using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct UnsubscribeRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.Unsubscribe;
        public ushort Version => 1;
        public uint CorrelationId { get; }
        public byte SubscriptionId { get; }
        public UnsubscribeRequest(uint correlationId, byte subscriptionId)
        {
            CorrelationId = correlationId;
            SubscriptionId = subscriptionId;
        }

        public int SizeNeeded => 5;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            WireFormatting.WriteByte(span.Slice(4), SubscriptionId);
            return 5;
        }
    }

    public readonly struct UnsubscribeResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Unsubscribe;
        public ushort Version => 1;
        public uint CorrelationId { get; }

        public ResponseCode Code { get; }

        private UnsubscribeResponse(uint correlationId, ResponseCode responseCode)
        {
            CorrelationId = correlationId;
            Code = responseCode;
        }
        
        internal static int Read(ReadOnlySequence<byte> frame, out UnsubscribeResponse command)
        {
            // Tag and version are not used yet.
            // WireFormatting.ReadUInt16(frame, out var tag);
            // WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadUInt32(frame.Slice(4), out var correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out var responseCode);
            command = new UnsubscribeResponse(correlation, (ResponseCode) responseCode);
            return 10;
        }
    }
}
