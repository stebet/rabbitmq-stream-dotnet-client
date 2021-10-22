using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslHandshakeRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.SaslHandshake;
        public ushort Version => 1;
        public uint CorrelationId { get; }
        public SaslHandshakeRequest(uint correlationId) => CorrelationId = correlationId;
        public int SizeNeeded => 4;
        public int Write(Span<byte> span) => WireFormatting.WriteUInt32(span, CorrelationId);
    }

    public readonly struct SaslHandshakeResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.SaslHandshake;
        public ushort Version => 1;
        public uint CorrelationId { get; }
        public string[] Mechanisms { get; }

        public SaslHandshakeResponse(uint correlationId, string[] mechanisms)
        {
            CorrelationId = correlationId;
            Mechanisms = mechanisms;
        }
        
        internal static int Read(ReadOnlySequence<byte> frame, out SaslHandshakeResponse command)
        {
            // Tag and version are not used yet.
            // WireFormatting.ReadUInt16(frame, out ushort tag);
            // WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            //WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode); // unused
            WireFormatting.ReadUInt32(frame.Slice(10), out uint numMechs);
            var mechs = new string[numMechs];
            int offset = 14;
            for (int i = 0; i < numMechs; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out string mech);
                mechs[i] = mech;
            }

            command = new SaslHandshakeResponse(correlation, mechs);
            return offset;
        }
    }
}
