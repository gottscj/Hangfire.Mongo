@echo off

set msbuild=""
if exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\MSBuild.exe" (
	set msbuild="C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\MSBuild.exe"
)
if exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\MSBuild.exe" (
	set msbuild="C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\MSBuild.exe"
)
if exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\MSBuild\15.0\Bin\MSBuild.exe" (
	set msbuild="C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\MSBuild\15.0\Bin\MSBuild.exe"
)
echo %msbuild%
if %msbuild% == "" (
	echo no MSBuild found.
	exit /B 1
)

set base_path=%cd%
set version_file_path = \Hangfire.Mongo\Properties\AssemblyInfo.cs
set artifacts_sources_path="%base_path%\artifacts\sources"
set artifacts_bin_path="%base_path%\artifacts\bin"
set artifacts_nuget_path="%base_path%\artifacts\nuget"
set sources_path="%base_path%\src"
set build_output="%artifacts_sources_path%\Hangfire.Mongo\bin\Release"
rem set buildNumber=%APPVEYOR_BUILD_NUMBER%;
rem set buildTag=%APPVEYOR_REPO_TAG%;
set buildNumber=0.3.2.227
set buildTag=0.3.2

rem BUILD
echo delete and create artifacts folders
rmdir artifacts /s /q

mkdir %artifacts_bin_path%
mkdir %artifacts_nuget_path%
mkdir %artifacts_sources_path%

echo copying source files to artifacts folder
xcopy /e /v /q %sources_path% %artifacts_sources_path%

rem TODO, update AssemblyInfo.cs

echo build project using selected MSBuild
%msbuild% %artifacts_sources_path%\Hangfire.Mongo\Hangfire.Mongo.csproj /t:Rebuild /t:pack /p:Configuration="Release"

echo copying build output to bin artifacts folder
xcopy /e /v /q %build_output% %artifacts_bin_path%

echo moving nuget to own folder in artifacts
move /y %artifacts_bin_path%\Hangfire.Mongo.%buildTag%.nupkg %artifacts_nuget_path%

exit /B 0