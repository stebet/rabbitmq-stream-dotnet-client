using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeleteRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.Delete;
        public ushort Version => 1;
        public string Stream { get; }
        public uint CorrelationId { get; }

        public DeleteRequest (uint correlationId, string stream)
        {
            CorrelationId = correlationId;
            Stream = stream;
        }
        
        public int SizeNeeded => 4 + WireFormatting.StringSize(Stream);

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            return 4 + WireFormatting.WriteString(span.Slice(4), Stream);
        }
    }
    
    public readonly struct DeleteResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Delete;
        public ushort Version => 1;
        public ResponseCode ResponseCode { get; }
        public uint CorrelationId { get; }

        public DeleteResponse(uint correlationId, ResponseCode responseCode)
        {
            CorrelationId = correlationId;
            ResponseCode = responseCode;
        }

        internal static int Read(ReadOnlySequence<byte> frame, out DeleteResponse command)
        {
            // Unused, so not spending time on reading them
            //var offset = WireFormatting.ReadUInt16(frame, out var tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadUInt32(frame.Slice(4), out var correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out var responseCode);
            command = new DeleteResponse(correlation, (ResponseCode)responseCode);
            return 10;
        }
    }
}
