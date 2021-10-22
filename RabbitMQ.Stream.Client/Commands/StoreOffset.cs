using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct StoreOffset : IClientCommand
    {
        public CommandKey Key => CommandKey.StoreOffset;
        public ushort Version => 1;
        public string Stream { get; }
        public string Reference { get; }
        public ulong OffsetValue { get; }

        public StoreOffset(string stream, string reference, ulong offsetValue)
        {
            Stream = stream;
            Reference = reference;
            OffsetValue = offsetValue;
        }

        public int SizeNeeded => 8 + WireFormatting.StringSize(Reference) + WireFormatting.StringSize(Stream);

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteString(span, Reference);
            offset += WireFormatting.WriteString(span.Slice(offset), Stream);
            return offset + WireFormatting.WriteUInt64(span.Slice(offset), OffsetValue);
        }
    }
}