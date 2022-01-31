using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rebus.Handlers;
using RebusOutboxWebApp.Messages;
#pragma warning disable CS1998

namespace RebusOutboxWebApp.Handlers
{
    public class SendMessageCommandHandler : IHandleMessages<SendMessageCommand>
    {
        private readonly ILogger<SendMessageCommandHandler> _logger;

        public SendMessageCommandHandler(ILogger<SendMessageCommandHandler> logger)
        {
            _logger = logger;
        }

        public async Task Handle(SendMessageCommand message)
        {
            _logger.LogInformation("Handling message {text}", message.Message);
        }
    }
}
