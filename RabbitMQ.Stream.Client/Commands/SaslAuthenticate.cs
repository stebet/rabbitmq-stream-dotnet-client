using System;
using System.Buffers;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslAuthenticateRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.SaslAuthenticate;
        public ushort Version => 1;
        public string Mechanism { get; }
        public Memory<byte> Data { get; }

        public uint CorrelationId { get; }

        public SaslAuthenticateRequest(uint correlationId, string mechanism, Memory<byte> data)
        {
            CorrelationId = correlationId;
            Mechanism = mechanism;
            Data = data;
        }

        public int SizeNeeded => 4 + WireFormatting.StringSize(Mechanism) + 4 + Data.Length;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt32(span, CorrelationId);
            int offset = 4 + WireFormatting.WriteString(span.Slice(4), Mechanism);
            return offset + WireFormatting.WriteBytes(span.Slice(offset), Data);
        }
    }

    public readonly struct SaslAuthenticateResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.SaslAuthenticate;
        public ushort Version => 1;

        public SaslAuthenticateResponse(uint correlationId, ushort code, byte[] data)
        {
            CorrelationId = correlationId;
            ResponseCode = code;
            Data = data;
        }

        public uint CorrelationId { get; }

        public ushort ResponseCode { get; }

        public byte[] Data { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out SaslAuthenticateResponse command)
        {
            // Tag and version aren't used yet.
            //var offset = WireFormatting.ReadUInt16(frame, out ushort tag);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            if (frame.Length > 10)
            {
                WireFormatting.ReadBytes(frame.Slice(10), out byte[] data);
                command = new SaslAuthenticateResponse(correlation, responseCode, data);
                return 14 + data.Length;
            }
            else
            {
                command = new SaslAuthenticateResponse(correlation, responseCode, Array.Empty<byte>());
                return 10;
            }
        }
    }
}
