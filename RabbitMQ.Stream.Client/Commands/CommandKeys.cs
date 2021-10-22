using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public enum CommandKey : ushort
    {
        None = 0,
        DeclarePublisher = 1,
        Publish = 2,
        PublishConfirm = 3,
        PublishError = 4,
        QueryPublisherSequence = 5,
        DeletePublisher = 6,
        Subscribe = 7,
        Deliver = 8,
        Credit = 9,
        StoreOffset = 10,
        QueryOffset = 11,
        Unsubscribe = 12,
        Create = 13,
        Delete = 14,
        Metadata = 15,
        MetadataUpdate = 16,
        PeerProperties = 17,
        SaslHandshake = 18,
        SaslAuthenticate = 19,
        Tune = 20,
        Open = 21,
        Close = 22,
        Heartbeat = 23,
    }
}