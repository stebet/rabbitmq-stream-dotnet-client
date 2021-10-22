using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CreateRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.Create;
        public ushort Version => 1;
        public string Stream { get; }
        public Dictionary<string, string> Arguments { get; }

        public uint CorrelationId { get; }

        public CreateRequest(uint correlationId, string stream, Dictionary<string, string> arguments)
        {
            CorrelationId = correlationId;
            Stream = stream;
            Arguments = arguments;
        }

        public int SizeNeeded
        {
            get
            {
                var argSize = 0;
                foreach (var (k, v) in Arguments)
                {
                    argSize += WireFormatting.StringSize(k) + WireFormatting.StringSize(v);
                }
                
                return 4 + WireFormatting.StringSize(Stream) + 4 + argSize;
            }
        }

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            int offset = 4 + WireFormatting.WriteString(span.Slice(4), Stream);
            offset += WireFormatting.WriteInt32(span.Slice(offset), Arguments.Count);
            foreach (var (k, v) in Arguments)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), k);
                offset += WireFormatting.WriteString(span.Slice(offset), v);
            }
                
            return offset;
        }
    }
    
    public readonly struct CreateResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.Create;
        public ushort Version => 1;
        private readonly ushort responseCode;
        
        public CreateResponse(uint correlationId, ushort responseCode)
        {
            CorrelationId = correlationId;
            this.responseCode = responseCode;
        }

        public uint CorrelationId { get; }

        public ResponseCode ResponseCode => (ResponseCode)responseCode;

        internal static int Read(ReadOnlySequence<byte> frame, out CreateResponse command)
        {
            // tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out var tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            WireFormatting.ReadUInt32(frame.Slice(4), out var correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out var responseCode);
            command = new CreateResponse(correlation, responseCode);
            return 10;
        }
    }
}
