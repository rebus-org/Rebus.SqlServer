using System;
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Local
// ReSharper disable UnusedAutoPropertyAccessor.Local

namespace RebusOutboxWebAppEfCore.Entities
{
    public class PostedMessage
    {
        public static PostedMessage New(string text) => new(text, DateTimeOffset.Now, PostedMessageState.Enqueued);

        public int Id { get; private set; }

        public string Text { get; private set; }

        public DateTimeOffset Time { get; private set; }

        public PostedMessageState State { get; private set; }

        public PostedMessage(string text, DateTimeOffset time, PostedMessageState state)
        {
            Text = text;
            State = state;
            Time = time;
        }

        public void ChangeState(PostedMessageState state)
        {
            State = state;
        }

        public enum PostedMessageState
        {
            Enqueued,
            Finished
        }
    }
}
