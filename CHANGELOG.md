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

----

[cleytonb]: https://github.com/cleytonb