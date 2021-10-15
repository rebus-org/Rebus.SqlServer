using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Rebus.Bus;
using RebusOutboxWebAppEfCore.Entities;
using RebusOutboxWebAppEfCore.Messages;
using RebusOutboxWebAppEfCore.Models;

namespace RebusOutboxWebAppEfCore.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly WebAppDbContext _context;
        private readonly IBus _bus;

        public HomeController(ILogger<HomeController> logger, IBus bus, WebAppDbContext context)
        {
            _logger = logger;
            _bus = bus;
            _context = context;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        [Route("send-message")]
        public async Task<IActionResult> SendMessage([FromBody] SendMessageForm form)
        {
            var postedMessage = PostedMessage.New(form.Message);
            
            await _context.PostedMessages.AddAsync(postedMessage);

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (Exception exception)
            {
                var details = exception.ToString();
                Console.WriteLine(details);
            }

            await _bus.Send(new ProcessMessageCommand(postedMessage.Id));

            return Ok();
        }

        public record SendMessageForm(string Message);
    }
}
