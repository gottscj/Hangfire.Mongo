@echo off

if "%1" == "" (
	echo No target framework provided.
	exit /B 1
)
dotnet test --no-build --no-restore --configuration:Release --framework:%1 --filter:Category!=DataGeneration artifacts\sources\Hangfire.Mongo.Tests\Hangfire.Mongo.Tests.csproj

