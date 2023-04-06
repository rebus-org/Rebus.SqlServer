using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.Examples;

[TestFixture]
[Description("Shows how it's possible to extract the currently ongoing SqlTransaction from an SqlConnection. This way of obtaining the transaction should probably not be relied on, AND it also fails to retrieve the SqlTransaction when the SqlConnection is enlisted in a TransactionScope")]
[Explicit]
public class TestSqlConnectionTransactionExtraction : FixtureBase
{
    [Test]
    public async Task CanGetIt()
    {
        await using var connection = new SqlConnection(SqlTestHelper.ConnectionString);

        await connection.OpenAsync();

        await using var transaction = connection.BeginTransaction();

        var reflectionTransaction = GetSqlTransactionViaReflection(connection);

        Assert.That(reflectionTransaction, Is.EqualTo(transaction));
    }

    [Test]
    public async Task CanGetIt_TransactionScope()
    {
        // we need to ensure that all initialization has been run before starting the TransactionScope
        await using (var initConnection = new SqlConnection(SqlTestHelper.ConnectionString))
        {
            await initConnection.OpenAsync();
        }

        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        await using var connection = new SqlConnection(SqlTestHelper.ConnectionString);
        await connection.OpenAsync();

        var reflectionTransaction = GetSqlTransactionViaReflection(connection);

        Assert.That(reflectionTransaction, Is.Not.Null);
    }

    static SqlTransaction GetSqlTransactionViaReflection(SqlConnection connection)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;

        DumpTypeInfo(connection);

        var innerConnectionField = connection.GetType().GetProperty("InnerConnection", flags);
        var innerConnection = innerConnectionField?.GetValue(connection);

        DumpTypeInfo(innerConnection);

        var availableInnerTransactionField = innerConnection?.GetType().GetProperty("AvailableInternalTransaction", flags);
        var availableInnerTransaction = availableInnerTransactionField?.GetValue(innerConnection);

        DumpTypeInfo(availableInnerTransaction);

        var parentTransactionField = availableInnerTransaction?.GetType().GetProperty("Parent", flags);
        var parentTransaction = parentTransactionField?.GetValue(availableInnerTransaction) as SqlTransaction;

        return parentTransaction;
    }

    static void DumpTypeInfo(object obj)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;

        if (obj == null) return;

        var privateFields = string.Join(Environment.NewLine, obj.GetType()
            .GetFields(flags)
            .OrderBy(f => f.Name)
            .Select(f => new
            {
                f.Name,
                f.FieldType
            }));

        var privateProperties = string.Join(Environment.NewLine, obj.GetType()
            .GetProperties(flags)
            .OrderBy(f => f.Name)
            .Select(f => new
            {
                f.Name,
                f.PropertyType
            }));

        Console.WriteLine($@"Private fields of {obj.GetType()}:

{privateFields}

Private properties of {obj.GetType()}

{privateProperties}");
    }
}