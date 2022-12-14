using System;
using System.Linq.Expressions;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.SqlServer.Tests.Extensions;

static class TestExtensions
{
    public static async Task WaitUntil<T>(this T subject, Expression<Func<T, bool>> successExpression, Expression<Func<T, bool>> failureExpression = null, int timeoutSeconds = 5)
    {
        if (subject == null) throw new ArgumentNullException(nameof(subject));
        if (successExpression == null) throw new ArgumentNullException(nameof(successExpression));

        var success = successExpression.Compile();
        var failure = failureExpression?.Compile() ?? (_ => false);

        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        var cancellationToken = cancellationTokenSource.Token;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (failure(subject)) throw new ApplicationException($@"The success expression

    {successExpression}

was not completed before detecting a failure via

    {failureExpression}");

                if (success(subject) && !failure(subject)) return;
                
                if (failure(subject)) throw new ApplicationException($@"The success expression

    {successExpression}

was not completed before detecting a failure via

    {failureExpression}");

                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new TaskCanceledException($@"The success expression

    {successExpression}

was not completed within {timeoutSeconds} s timeout");
        }
    }
}