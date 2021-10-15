using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rebus.Handlers;
using RebusOutboxWebAppEfCore.Entities;
using RebusOutboxWebAppEfCore.Messages;

namespace RebusOutboxWebAppEfCore.Handlers
{
    public class ProcessMessageCommandHandler : IHandleMessages<ProcessMessageCommand>
    {
        private readonly ILogger<ProcessMessageCommandHandler> _logger;
        private readonly WebAppDbContext _context;

        public ProcessMessageCommandHandler(ILogger<ProcessMessageCommandHandler> logger, WebAppDbContext context)
        {
            _logger = logger;
            _context = context;
        }

        public async Task Handle(ProcessMessageCommand message)
        {
            var id = message.MessageId;

            var postedMessage = await _context.PostedMessages.FindAsync(id)
                                ?? throw new ArgumentException($"Could not find message with ID '{id}'");

            postedMessage.ChangeState(PostedMessage.PostedMessageState.Finished);

            _logger.LogInformation("Successfully handled posted message with ID {postedMessageId}", id);
        }
    }
}
