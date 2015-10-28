# Simple.Data.Firebird
Simple.Data.Firebird is an adapter for Simple.Data framework. 

# Use
Easiest way to install is to use a NuGet package (https://www.nuget.org/packages/Simple.Data.Firebird). Firebird ADO.NET Data provider is also required.

####Note about bulk insert 

Since Firebird does not support sending multiple insert commands at once, execute block statement is used to speed up insert process. Because of Firebird limitations it's possible to speed up bulk insert even more (up to 2x-3x faster depending on data and table schema) by not using command parameters and placing insert values directly in sql. While single quotes are escaped, it is still potentially unsafe, that's why this feature is disabled by default and may enabled by calling:

```C#
Simple.Data.Firebird.BulkInsertConfiguration.UseFasterUnsafeBulkInsertMethod = true;
```
   
# Testing
Adapter was tested against Firebird 2.5. By default, tests use user 'SYSDBA' with 'masterkey' password. It can easily be changed in App.config in connectionStringTemplate key.

New database file is created on each test run and old one is dropped if it exists. This ensures that previous test runs side effects does not impact current test results.

#Additional info
Most of tests methods and a few features are based on Simple.Data.PostgreSql by Chris Hogan.
