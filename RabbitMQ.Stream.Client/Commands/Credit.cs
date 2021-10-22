using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Credit : IClientCommand
    {
        public CommandKey Key => CommandKey.Credit;
        public ushort Version => 1;
        public byte SubscriptionId { get; }
        public ushort CreditAmount { get; }

        public Credit(byte subscriptionId, ushort credit)
        {
            SubscriptionId = subscriptionId;
            CreditAmount = credit;
        }

        public int SizeNeeded => 3;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteByte(span, SubscriptionId);
            WireFormatting.WriteUInt16(span.Slice(1), CreditAmount);
            return 3;
        }
    }
}