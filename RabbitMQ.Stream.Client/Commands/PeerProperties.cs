using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PeerPropertiesRequest : ICommandRequest
    {
        public CommandKey Key => CommandKey.PeerProperties;
        public ushort Version => 1;
        public Dictionary<string, string> Properties { get; }
        public uint CorrelationId { get; }

        public PeerPropertiesRequest(uint correlationId, Dictionary<string, string> properties)
        {
            CorrelationId = correlationId;
            Properties = properties;
        }

        public int SizeNeeded
        {
            get
            {
                int size = 8;
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
            // map
            WireFormatting.WriteInt32(span.Slice(4), Properties.Count);
            int offset = 8;
            foreach(var (k,v) in Properties)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), k);
                offset += WireFormatting.WriteString(span.Slice(offset), v);
            }

            return offset;
        }
    }

    public readonly struct PeerPropertiesResponse : ICommandResponse
    {
        public CommandKey Key => CommandKey.PeerProperties;
        public ushort Version => 1;

        public PeerPropertiesResponse(uint correlationId, Dictionary<string, string> properties, ushort responseCode)
        {
            ResponseCode = responseCode;
            CorrelationId = correlationId;
            Properties = properties;
        }

        public uint CorrelationId { get; }
        public Dictionary<string, string> Properties { get; }

        public ushort ResponseCode { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out PeerPropertiesResponse command)
        {
            // Tag and version aren't used yet.
            //WireFormatting.ReadUInt16(frame, out ushort tag);
            //WireFormatting.ReadUInt16(frame.Slice(offset), out ushort version);
            WireFormatting.ReadUInt32(frame.Slice(4), out uint correlation);
            WireFormatting.ReadUInt16(frame.Slice(8), out ushort responseCode);
            WireFormatting.ReadInt32(frame.Slice(10), out int numProps);
            int offset = 14;
            var props = new Dictionary<string, string>(numProps);
            for (int i = 0; i < numProps; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out string k);
                offset += WireFormatting.ReadString(frame.Slice(offset), out string v);
                props.Add(k, v);
            }
            command = new PeerPropertiesResponse(correlation, props, responseCode);
            return offset;
        }
    }
}
