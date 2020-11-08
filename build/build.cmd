@echo off

set community_msbuild=C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\MSBuild\Current\Bin\MSBuild.exe
set proofessional_msbuild=C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\MSBuild\Current\Bin\MSBuild.exe
set enterprise_msbuild=C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\MSBuild\Current\Bin\MSBuild.exe

set msbuild=""
if exist "%community_msbuild%" (
	set msbuild="%community_msbuild%"
) else if exist "%proofessional_msbuild%" (
	set msbuild="%proofessional_msbuild%"
) else if exist "%enterprise_msbuild%" (
	set msbuild="%enterprise_msbuild%"
)

set artifacts_sources_path="%cd%\artifacts\sources"
set artifacts_bin_path="%cd%\artifacts\bin"
set artifacts_nuget_path="%cd%\artifacts\nuget"
set sources_path="%cd%\src"
set build_output="%artifacts_sources_path%\Hangfire.Mongo\bin\Any CPU\Release"
set nuget="%sources_path%\.nuget\NuGet.exe"

if %msbuild% == "" (
	echo No MSBuild found.
	exit /B 1
)

rem BUILD
echo Restoring NuGet packages...
%nuget% restore %sources_path%\Hangfire.Mongo.sln

echo build project using selected MSBuild
%msbuild% %sources_path%\Hangfire.Mongo.sln /t:Rebuild /p:Configuration="Release" /p:Platform="Any CPU"
if errorlevel 1 (
	echo Build failed.
	exit /B 1
)

rem CREATE ARTIFACTS
echo delete and create artifacts folders
rmdir artifacts /s /q

mkdir %artifacts_bin_path%
mkdir %artifacts_nuget_path%
mkdir %artifacts_sources_path%

echo copying source files to artifacts folder
xcopy /e /v /q /f %sources_path% %artifacts_sources_path%

echo Restoring NuGet packages...
%nuget% restore %artifacts_sources_path%\Hangfire.Mongo\Hangfire.Mongo.csproj

echo build project using selected MSBuild
%msbuild% %artifacts_sources_path%\Hangfire.Mongo\Hangfire.Mongo.csproj /t:Rebuild /t:pack /p:Configuration="Release" /p:Platform="Any CPU"

if errorlevel 1 (
	echo Build failed.
	exit /B 1
)

echo copying build output to bin artifacts folder
xcopy /e /v /q /f %build_output% %artifacts_bin_path%

echo moving nuget to own folder in artifacts
move /y %artifacts_bin_path%\*.nupkg %artifacts_nuget_path%

exit /B 0
