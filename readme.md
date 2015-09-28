# Simple.Data.Firebird
Simple.Data.Firebird is an adapter for Simple.Data framework. 

# Use
Easiest way to install is to use a NuGet package (https://www.nuget.org/packages/Simple.Data.Firebird). Firebird ADO.NET Data provider is also required.

# Testing
Adapter was tested against Firebird 2.5. By default, tests use user 'SYSDBA' with 'masterkey' password. It can easily be changed in App.config in connectionStringTemplate key.

New database file is created on each test run and old one is dropped if it exists. This ensures that previous test runs side effects does not impact current test results.

#Additional info
Most of tests methods and a few features are based on Simple.Data.PostgreSql by Chris Hogan.
