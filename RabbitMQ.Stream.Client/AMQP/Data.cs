using System;
using System.Buffers;
using System.Threading;

namespace RabbitMQ.Stream.Client.AMQP
{
    public interface IWritable
    {
        public int Size { get; }
        public int Write(Span<byte> span);
    }
    
    public readonly struct Data : IWritable
    {
        public Data(ReadOnlyMemory<byte> data)
        {
            Contents = data;
        }

        public ReadOnlyMemory<byte> Contents { get; }

        public int Size
        {
            get
            {
                if (Contents.Length < 256)
                    return (int)Contents.Length + 5;
                return (int) Contents.Length + 8;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteByte(span, 0); //descriptor marker
            offset += WireFormatting.WriteByte(span.Slice(offset), 0x53); //short ulong
            offset += WireFormatting.WriteByte(span.Slice(offset), 117); //data code number
            if (Contents.Length < 256)
            {
                offset += WireFormatting.WriteByte(span.Slice(offset), 0xA0); //binary marker
                offset += WireFormatting.WriteByte(span.Slice(offset), (byte)Contents.Length); //length
            }
            else
            {
                offset += WireFormatting.WriteByte(span.Slice(offset), 0xB0); //binary marker
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)Contents.Length); //length
            }

            offset += WireFormatting.Write(span.Slice(offset), Contents);
            return offset;
        }

        public static Data Parse(ReadOnlyMemory<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData.Span, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset).Span, out var descriptor);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset).Span, out var dataCode);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset).Span, out var binaryMarker);
            switch (binaryMarker)
            {
                case 0xA0:
                {
                    offset += WireFormatting.ReadByte(amqpData.Slice(offset).Span, out var length);
                    return new Data(amqpData.Slice(offset, length));
                }
                case 0xB0:
                {
                    offset += WireFormatting.ReadUInt32(amqpData.Slice(offset).Span, out var length);
                    return new Data(amqpData.Slice(offset, (int)length));
                }
            }

            throw new AmqpParseException("failed to parse data");
        }
    }
}