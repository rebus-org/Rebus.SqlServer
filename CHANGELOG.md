# Changelog

## 2.0.0-b01
* Test release

## 2.0.0-b02
* Use more convenient namespaces

## 2.0.0-b03
* Optimized streamed reading of data

## 2.0.0-b04
* Dispose connection properly also when an error occurs

## 2.0.0
* Release 2.0.0

## 2.1.0-b01
* Add support for schemas to all SQL Server-based things - thanks [cleytonb]

## 2.1.0-b02
* Topic/subscriber address length checks on subscription storage because SQL Server is crazy
* Support custom topic/subscriber address lengths in subscription storage via simple schema reflection at startup

## 3.0.0
* Update to Rebus 3

## 3.1.0
* Register SQL Transport as if it was an external timeout manager, enabling message deferral from one-way clients

## 3.1.1
* Fix unspecified saga revision after delete (fixes clash with SQL Server saga auditing assumption that revision be incremented also on delete...)

## 4.0.0
* Update to Rebus 4
* Add .NET Core support (netstandard1.6)
* Made `CurrentConnectionKey` of `SqlTransport` public

## 5.0.0
* Add lease-based transport - thanks [MrMDavidson]
* Add creation time column to data bus storage - thanks [IsaacSee]
* Shift transports to use table-per-queue layout to improve overall performance and avoid having a long queue bring everything to a halt - thanks [magnus-tretton37]
* Change meaning of the secret and pretty un-documented `rbs2-msg-priority` header to work like it seems most people expect it to: Higher number means higher priority
* Change all `datetime2` column types to be `datetimeoffset` instead to make data less tied to the SQL Server instance (probably "server time of ...") that created it
* Retry schema generation if it fails, most likely because of race condition between checking for the existence of a table and trying to create it
* Add ambient transaction support, so 2PC can be enabled if one desires - thanks [larsw]
* Fix double-disposal of renew timer - thanks [MrMDavidson]
* `ConfigureAwait(false)` in places that cannot result in callbacks to Rebus extensibility points
* Change receive ordering such that the visiblity time takes precedence over insertion time. This way, messages deferred at different times into the future will arrive in the scheduled order and not risk being received out-of-order because they happen to have been inserted that way
* Add `READCOMMITTEDLOCK` hint such that receive locks work properly also when running the database with snapshot isolation enabled
* Target .NET 4.5, .NET 4.6, and .NET Standard 2.0

## 5.0.1
* Reduce query plan cache pollution by coming up with lengths for all parameterized queries

## 5.0.2
* Improve error message when registering a timeout manager in conjunction with the SQL transport (which has built-in message delay, obviating the need for actual timeout storage)

## 5.0.3
* Correct name of connection string parameter in configuration extensions

## 5.0.4
* Add `READCOMMITTEDLOCK` also to the receive code of the lease-based transport

## 5.0.5
* Fix problem with closed connection when enlisting in ambient transaction - thanks [thomasdc]

## 5.1.0
* Change default data type of timeouts table PK IDs to be `BIGINT` - thanks [rsivanov]

## 6.0.0
* Update Rebus dependency to version 6 - thanks [nativenolde]
* Make it possible to avoid automatic schema creation - thanks [MrMDavidson]
* Simplify configuration API and deprecate swarm of old overloads
* Implement new data bus storage APIs that enable querying and deleting
* Add two more indexes to improve performance and resource utilization of lease-based transport - thanks [MrMDavidson]
* Reduce implicit conversions and improve precision in SQL transport - thanks [tompazourek]

## 6.0.1
* Fix bug that would ignore the setting for customizing the delay between cleanup cycles

## 6.1.0
* Add ability to auto-delete input queue when shutting down - thanks [rsivanov]

## 6.1.1
* Fix bug that would not make column types for "leaseduntil" and "leasedat" DATETIMEOFFSET as intended

## 6.1.2
* Catch and log exceptions caught when failing to update the lease for long-running message handlers and lease-based transport - thanks [clegendre]

## 7.0.0
* Replace dependency on System.Data.SqlClient with Microsoft.Data.SqlClient, because it's the future!
* Only target .NET Standard 2.0, because requiring consumers to be on .NET 4.6.2 or .NET Core 2 is fair

## 7.0.1
* Modify receive index for better performance - thanks [seankearon]

## 7.1.0
* Add `ISagaSerializer` thus making it possible to customize how saga data is serialized - thanks [mathiasnohall]

## 7.1.1
* Don't leave connection with NOCOUNT ON in the transport - can interfere with other stuff using the connection

## 7.1.2
* Adjustments to the new saga data serializer: Pass type to deserialize method (note: this is breaking, if you've implemented your own `ISagaSerializer`) - thanks [mathiasnohall]

## 7.1.3
* Correct XML docs on `DbConnectionFactoryProvider` (were copy/pasted from another implementation of `IDbConnectionProvider`), remove isolation level property from it (wasn't used), and remove logging antipattern

## 7.1.4
* Add necessary overloaded constructor to `SqlServerTransportOptions` to enable resolving an `IDbConnectionProvider` implementation that requires other things (e.g. `IRebusLoggerFactory`). Also add `SqlConnectionOpening` callback to `DbConnectionProvider`, which makes for a good place to set the `AccessToken` property of `SqlConnection`

## 7.1.5
* Add addition configuration overloads to allow for consistent level of customization across persisters

## 7.1.6
* Fix bug in saga data serializer configuration - thanks [mathiasnohall]

## 7.1.7
* Fix disposal of data reader when read attempt does not find anything and must throw

## 7.2.0
* Make it possible to disable native message defer functionality of SQL transport, thus making it possible to run a dedicated SQL timeout manager (like with any other transport) together with the SQL transport
* Migrate old receive index to new, improved index - thanks [Simplicitdk]

----

[clegendre]: https://github.com/clegendre
[cleytonb]: https://github.com/cleytonb
[IsaacSee]: https://github.com/IsaacSee
[larsw]: https://github.com/larsw
[magnus-tretton37]: https://github.com/magnus-tretton37
[mathiasnohall]: https://github.com/mathiasnohall
[MrMDavidson]: https://github.com/MrMDavidson
[nativenolde]: https://github.com/nativenolde
[rsivanov]: https://github.com/rsivanov
[seankearon]: https://github.com/seankearon
[Simplicitdk]: https://github.com/Simplicitdk
[thomasdc]: https://github.com/thomasdc
[tompazourek]: https://github.com/tompazourek

