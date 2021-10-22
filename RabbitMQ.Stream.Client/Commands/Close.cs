using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CloseRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.Close;
        public ushort Version => 1;
        public string Reason { get; }
        public uint CorrelationId { get; }

        public CloseRequest(uint correlationId, string reason)
        {
            CorrelationId = correlationId;
            Reason = reason;
        }

        public int SizeNeeded => 6 + WireFormatting.StringSize(Reason);

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            WireFormatting.WriteUInt16(span.Slice(4), 1); //ok code
            return 6 + WireFormatting.WriteString(span.Slice(6), Reason);
        }
    }

    public readonly struct CloseResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Close;
        public ushort Version => 1;
        public uint CorrelationId { get; }
        public ushort ResponseCode { get; }

        public CloseResponse(uint correlationId, ushort responseCode)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
        }
        
        internal static int Read(ReadOnlySequence<byte> frame, out CloseResponse command)
        {
            // tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            command = new CloseResponse(correlation, responseCode);
            return 10;
        }
    }
}
