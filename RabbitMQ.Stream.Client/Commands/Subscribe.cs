using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public enum OffsetTypeEnum
    {
        First = 1,
        Last = 2,
        Next = 3,
        Offset = 4,
        Timestamp = 5
    }

    public interface IOffsetType
    {
        int Size { get; }

        OffsetTypeEnum OffsetType { get; }

        int Write(Span<byte> span);
    }

    public readonly struct OffsetTypeFirst : IOffsetType
    {
        public int Size => 2;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.First;
        public int Write(Span<byte> span) => WireFormatting.WriteUInt16(span, (ushort)OffsetType);
    }
    public readonly struct OffsetTypeLast : IOffsetType
    {
        public int Size => 2;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Last;
        public int Write(Span<byte> span) => WireFormatting.WriteUInt16(span, (ushort)OffsetType);
    }
    public readonly struct OffsetTypeNext : IOffsetType
    {
        public int Size => 2;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Next;
        public int Write(Span<byte> span) => WireFormatting.WriteUInt16(span, (ushort)OffsetType);
    }

    public readonly struct OffsetTypeOffset : IOffsetType
    {
        private readonly ulong offsetValue;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Offset;
        public OffsetTypeOffset(ulong offset)
        {
            offsetValue = offset;
        }

        public int Size => 2 + 8;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            return 2 + WireFormatting.WriteUInt64(span.Slice(2), offsetValue);
        }
    }

    public readonly struct OffsetTypeTimestamp : IOffsetType
    {
        private readonly long timestamp;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Timestamp;
        public OffsetTypeTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
        }

        public int Size => 10;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            return 2 + WireFormatting.WriteInt64(span.Slice(2), timestamp);
        }
    }

    public readonly struct SubscribeRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.Subscribe;
        public ushort Version => 1;
        public byte SubscriptionId { get; }
        public string Stream{get;}
        public IOffsetType OffsetType{get;}
        public ushort Credit{get;}
        public Dictionary<string, string> Properties { get; }

        public uint CorrelationId { get; }

        public SubscribeRequest(uint correlationId, byte subscriptionId, string stream, IOffsetType offsetType, ushort credit, Dictionary<string, string> properties)
        {
            CorrelationId = correlationId;
            SubscriptionId = subscriptionId;
            Stream = stream;
            OffsetType = offsetType;
            Credit = credit;
            Properties = properties;
        }

        public int SizeNeeded
        {
            get
            {
                int size = 4 + 1 + WireFormatting.StringSize(Stream) + OffsetType.Size + 2 + 4;
                foreach (var (k, v) in Properties)
                {
                    // TODO: unnecessary conversion work here to work out the correct size of the frame
                    size += WireFormatting.StringSize(k) + WireFormatting.StringSize(v); //
                }

                return size;
            }
        }

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            WireFormatting.WriteByte(span.Slice(4), SubscriptionId);
            int offset = 5 + WireFormatting.WriteString(span.Slice(5), Stream);
            offset += OffsetType.Write(span.Slice(offset));
            offset += WireFormatting.WriteUInt16(span.Slice(offset), Credit);
            offset += WireFormatting.WriteInt32(span.Slice(offset), Properties.Count);
            foreach (var (k, v) in Properties)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), k);
                offset += WireFormatting.WriteString(span.Slice(offset), v);
            }

            return offset;
        }
    }

    public readonly struct SubscribeResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Subscribe;
        public ushort Version => 1;

        private SubscribeResponse(uint correlationId, ResponseCode responseCode)
        {
            CorrelationId = correlationId;
            Code = responseCode;
        }

        public uint CorrelationId { get; }

        public ResponseCode Code { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out SubscribeResponse command)
        {
            // Tag and version are not used yet
            // WireFormatting.ReadUInt16(frame, out var tag);
            // WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadUInt32(frame.Slice(4), out var correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out var responseCode);
            command = new SubscribeResponse(correlation, (ResponseCode)responseCode);
            return 10;
        }
    }
}