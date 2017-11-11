using System;
using System.Collections.Generic;

namespace Sample.Service.UTILS.Logging.Abstractions.Data
{
    public class MessageData
    {
        public DateTime Timestamp         { get; }
        public string   CategoryName      { get; set; }
        public int      EventId           { get; set; }
        public string   EventName         { get; set; }
        public string   LogLevel          { get; set; }
        public string   Message           { get; set; }
        public string   Exception         { get; set; }
        public string   ExceptionMessages { get; set; }
        public string   StackTrace        { get; set; }
        public string   TraceIdentifier   { get; set; }
        public IEnumerable<string> Tags { get; set; }

        public MessageData()
        {
            Timestamp = DateTime.UtcNow;
        }
    }
}
