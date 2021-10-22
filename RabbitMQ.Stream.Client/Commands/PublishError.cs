using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishError : IServerCommand
    {
        public CommandKey Key => CommandKey.PublishError;
        public ushort Version => 1;
        public uint CorrelationId => uint.MaxValue;
        public byte PublisherId { get; }
        public (ulong, ResponseCode)[] PublishingErrors { get; }

        private PublishError(byte publisherId, (ulong, ResponseCode)[] publishingErrors)
        {
            PublisherId = publisherId;
            PublishingErrors = publishingErrors;
        }
        
        internal static int Read(ReadOnlySequence<byte> frame, out PublishError command)
        {
            WireFormatting.ReadUInt16(frame, out var tag);
            WireFormatting.ReadUInt16(frame.Slice(2), out var version);
            WireFormatting.ReadByte(frame.Slice(4), out var publisherId);
            WireFormatting.ReadInt32(frame.Slice(5), out var numErrors);
            int offset = 9;
            var publishingIds = new (ulong, ResponseCode)[numErrors];
            for (var i = 0; i < numErrors; i++)
            {
                offset += WireFormatting.ReadUInt64(frame.Slice(offset), out var pubId);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var code);
                publishingIds[i] = (pubId, (ResponseCode)code);
            }
            command = new PublishError(publisherId, publishingIds);
            return offset;
        }
    }
}
