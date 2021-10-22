using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Publish : IClientCommand
    {
        public CommandKey Key => CommandKey.Publish;
        public ushort Version => 1;

        public int SizeNeeded
        {
            get
            {
                var size = 5; // pre amble 
                foreach (var (_, msg) in Messages)
                {
                    size += 8 + 4 + msg.Size;
                }
                
                return size;
             }
        }

        public byte PublisherId { get; }
        public List<(ulong, Message)> Messages { get; }

        public Publish(byte publisherId, List<(ulong, Message)> messages)
        {
            PublisherId = publisherId;
            Messages = messages;
        }

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteByte(span, PublisherId);
            // this assumes we never write an empty publish frame
            WireFormatting.WriteInt32(span.Slice(1), Messages.Count);
            int offset = 5;
            foreach(var (publishingId, msg) in Messages)
            {
                offset += WireFormatting.WriteUInt64(span.Slice(offset), publishingId);
                // this only write "simple" messages, we assume msg is just the binary body
                // not stream encoded data
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            return offset;
        }
    }
}
