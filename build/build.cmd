@echo off

set community_msbuild=C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\MSBuild.exe
set proofessional_msbuild=C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\MSBuild.exe
set enterprise_msbuild=C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\MSBuild\15.0\Bin\MSBuild.exe

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
set build_output="%artifacts_sources_path%\Hangfire.Mongo\bin\Release"

echo build number : %APPVEYOR_BUILD_NUMBER%
echo build tag    : %APPVEYOR_REPO_TAG%
echo build version: %APPVEYOR_BUILD_VERSION%

if %msbuild% == "" (
	echo No MSBuild found.
	exit /B 1
)

rem BUILD
echo delete and create artifacts folders
rmdir artifacts /s /q

mkdir %artifacts_bin_path%
mkdir %artifacts_nuget_path%
mkdir %artifacts_sources_path%

echo copying source files to artifacts folder
xcopy /e /v /q /f %sources_path% %artifacts_sources_path%

echo build project using selected MSBuild
%msbuild% %artifacts_sources_path%\Hangfire.Mongo\Hangfire.Mongo.csproj /t:Rebuild /t:pack /p:Configuration="Release"

echo copying build output to bin artifacts folder
xcopy /e /v /q /f %build_output% %artifacts_bin_path%

echo moving nuget to own folder in artifacts
move /y %artifacts_bin_path%\*.nupkg %artifacts_nuget_path%

exit /B 0