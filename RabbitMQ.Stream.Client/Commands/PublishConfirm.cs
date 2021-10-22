using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishConfirm : IServerCommand
    {
        public CommandKey Key => CommandKey.PublishConfirm;
        public ushort Version => 1;
        public uint CorrelationId => uint.MaxValue;

        private PublishConfirm(byte publisherId, ReadOnlyMemory<ulong> publishingIds)
        {
            PublisherId = publisherId;
            PublishingIds = publishingIds;
        }

        public byte PublisherId { get; }

        public ReadOnlyMemory<ulong> PublishingIds { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out PublishConfirm command)
        {
            // Tag and version aren't used yet.
            //WireFormatting.ReadUInt16(frame, out var tag);
            //WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadByte(frame.Slice(4), out var publisherId);
            WireFormatting.ReadInt32(frame.Slice(5), out var numIds);
            var publishingIds = new Memory<ulong>(ArrayPool<ulong>.Shared.Rent(numIds), 0, numIds);
            for (var i = 0; i < numIds; i++)
            {
                WireFormatting.ReadUInt64(frame.Slice(9+i*8), out ulong publishingId);
                publishingIds.Span[i] = publishingId;
            }

            command = new PublishConfirm(publisherId, publishingIds);
            return 9 + numIds * 8;
        }
    }
}
