using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using RebusOutboxWebApp.Models;
using System.Diagnostics;
using System.Threading.Tasks;
using Rebus.Bus;
using RebusOutboxWebApp.Messages;

namespace RebusOutboxWebApp.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IBus _bus;

        public HomeController(ILogger<HomeController> logger, IBus bus)
        {
            _logger = logger;
            _bus = bus;
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
            await _bus.Send(new SendMessageCommand(form.Message));

            return Ok();
        }

        public record SendMessageForm(string Message);
    }
}
