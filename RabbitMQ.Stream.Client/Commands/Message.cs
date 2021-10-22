using System;
using System.Buffers;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Message
    {
        public Properties Properties { get; }
        
        public Message(Memory<byte> data) : this(new Data(data))
        {
        }

        public Message(Data data, Properties properties = new Properties())
        {
            Data = data;
            Properties = properties;
        }

        public Data Data { get; }
        public int Size => Data.Size;

        public int Write(Span<byte> span) => Data.Write(span);

        public ReadOnlySequence<byte> Serialize()
        {
            //what a massive cludge
            var data = new byte[Data.Size];
            Data.Write(data);
            return new ReadOnlySequence<byte>(data);
        }

        public static Message From(ReadOnlyMemory<byte> amqpData)
        {
            //parse AMQP encoded data
            var data = AMQP.Data.Parse(amqpData);
            return new Message(data);
        }
    }
}