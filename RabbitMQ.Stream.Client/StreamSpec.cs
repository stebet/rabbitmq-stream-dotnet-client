using System;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public record StreamSpec
    {
        public StreamSpec(string name)
        {
            Name = name;
        }

        public string Name { get; init; }
        public TimeSpan MaxAge { set => Args["max-age"] = $"{value.TotalSeconds}s"; }
        public int MaxLengthBytes { set => Args["max-length-bytes"] = $"{value.ToString()}"; }
        public LeaderLocator LeaderLocator { set => Args["queue-leader-locator"] = $"{value.ToString()}"; }

        public Dictionary<string, string> Args { get; } = new Dictionary<string, string>();
    }
}