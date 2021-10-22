using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Tune : ICommandRequest, ICommandResponse
    {
        public CommandKey Key => CommandKey.Tune;
        public ushort Version => 1;
        public uint CorrelationId => uint.MaxValue;
        public uint FrameMax { get; }
        public uint Heartbeat { get; }
        public int SizeNeeded => 8;

        public Tune(uint frameMax, uint heartbeat)
        {
            FrameMax = frameMax;
            Heartbeat = heartbeat;
        }
        
        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, FrameMax);
            WireFormatting.WriteUInt32(span.Slice(4), Heartbeat);
            return 8;
        }

        internal static int Read(ReadOnlySequence<byte> frame, out Tune command)
        {
            // Tag and version are not used yet.
            //WireFormatting.ReadUInt16(frame, out ushort tag);
            //WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint frameMax);
            WireFormatting.ReadUInt32(frame.Slice(8), out uint heartbeat);
            command = new Tune(frameMax, heartbeat);
            return 12;
        }
    }
}