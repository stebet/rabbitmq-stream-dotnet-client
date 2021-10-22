using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    //Deliver => Key Version SubscriptionId OsirisChunk
    //   Key => uint16 // 8
    //   Version => uint32
    //   SubscriptionId => uint8
    public readonly struct Deliver : IServerCommand
    {
        public CommandKey Key => CommandKey.Deliver;
        public ushort Version => 1;
        public uint CorrelationId => uint.MaxValue;

        private Deliver(byte subscriptionId, Chunk chunk)
        {
            SubscriptionId = subscriptionId;
            Chunk = chunk;
        }

        public IEnumerable<MsgEntry> Messages
        {
            get
            {
                var offset = 0;
                var data = Chunk.Data;
                for (ulong i = 0; i < Chunk.NumEntries; i++)
                {
                    offset += WireFormatting.ReadUInt32(data.Slice(offset).Span, out var len);
                    //TODO: assuming only simple entries for now
                    var entry = new MsgEntry(Chunk.ChunkId + i, Chunk.Epoch, data.Slice(offset, (int)len));
                    offset += (int) len;
                    yield return entry;
                }
            }
        }

        public Chunk Chunk { get; }

        public byte SubscriptionId { get; }

        internal static int Read(ref ReadOnlyMemory<byte> frame, out Deliver command)
        {
            // tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out var tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadByte(frame.Slice(4).Span, out byte subscriptionId);
            var offset = 5 + Chunk.Read(frame.Slice(5), out Chunk chunk);
            command = new Deliver(subscriptionId, chunk);
            return offset;
        }
    }

    public readonly struct MsgEntry
    {
        public MsgEntry(ulong offset, ulong epoch, ReadOnlyMemory<byte> data)
        {
            Offset = offset;
            Epoch = epoch;
            Data = data;
        }

        public ulong Offset { get; }

        public ulong Epoch { get; }

        public ReadOnlyMemory<byte> Data { get; }
    }

    public readonly struct Chunk
    {
        private Chunk(byte magicVersion,
            ushort numEntries,
            uint numRecords,
            long timestamp,
            ulong epoch,
            ulong chunkId,
            int crc,
            ReadOnlyMemory<byte> data)
        {
            MagicVersion = magicVersion;
            NumEntries = numEntries;
            NumRecords = numRecords;
            Timestamp = timestamp;
            Epoch = epoch;
            ChunkId = chunkId;
            Crc = crc;
            Data = data;
        }
        
        public byte MagicVersion { get; }
        public ushort NumEntries { get; }
        public uint NumRecords { get; }
        public long Timestamp { get; }
        public ulong Epoch { get; }
        public ulong ChunkId { get; }
        public int Crc { get; }
        public ReadOnlyMemory<byte> Data { get; }

        internal static int Read(ReadOnlyMemory<byte> seq, out Chunk chunk)
        {
            WireFormatting.ReadByte(seq.Span, out var magicVersion);
            // WireFormatting.ReadByte(seq.Slice(1), out var chunkType); // not used
            WireFormatting.ReadUInt16(seq.Slice(2).Span, out var numEntries);
            WireFormatting.ReadUInt32(seq.Slice(4).Span, out var numRecords);
            WireFormatting.ReadInt64(seq.Slice(8).Span, out var timestamp);
            WireFormatting.ReadUInt64(seq.Slice(16).Span, out var epoch);
            WireFormatting.ReadUInt64(seq.Slice(24).Span, out var chunkId);
            WireFormatting.ReadInt32(seq.Slice(32).Span, out var crc);
            WireFormatting.ReadUInt32(seq.Slice(36).Span, out var dataLen);
            // WireFormatting.ReadUInt32(seq.Slice(40), out var trailerLen); // not used
            // offset += 4; // reserved
            //TODO: rather than copying at this point we may want to do codec / message parsing here
            var data = seq.Slice(48, (int)dataLen);
            chunk = new Chunk(magicVersion, numEntries, numRecords, timestamp, epoch, chunkId, crc, data);
            return 48 + (int)dataLen;
        }
    }
}
