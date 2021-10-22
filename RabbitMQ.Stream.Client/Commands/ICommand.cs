using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public interface IServerCommand
    {
        public CommandKey Key { get; }
        public ushort Version { get; }
    }

    public interface IClientCommand
    {
        public CommandKey Key { get; }
        public ushort Version { get; }
        public int SizeNeeded { get; }
        int Write(Span<byte> span);
    }

    public interface ICommandRequest : IClientCommand
    {
        public uint CorrelationId { get; }
    }

    public interface ICommandResponse
    {
        public CommandKey Key { get; }
        public ushort Version { get; }
        public uint CorrelationId { get; }
    }
}
