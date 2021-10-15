using Microsoft.EntityFrameworkCore;

namespace RebusOutboxWebAppEfCore.Entities
{
    public class WebAppDbContext : DbContext
    {
        public WebAppDbContext(DbContextOptions<WebAppDbContext> options) : base(options)
        {
        }

        public DbSet<PostedMessage> PostedMessages { get; set; }
    }
}
