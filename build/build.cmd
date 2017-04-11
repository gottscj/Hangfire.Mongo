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
set build_number=%APPVEYOR_BUILD_NUMBER%
set build_tag=%APPVEYOR_REPO_TAG%

if %build_number%=="" (
	echo AppVeyor APPVEYOR_BUILD_NUMBER vas not found!
	set build_number=0.3.2.227
	exit /B 1
)

if %build_tag%=="" (
	echo AppVeyor APPVEYOR_REPO_TAG vas not found!
	set build_tag=0.3.2
	exit /B 1
)

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

echo Updating Version information
call :UpdateAssemblyInfo
if %errorlevel%==1 (
	exit /B %errorlevel%
)
echo build project using selected MSBuild
%msbuild% %artifacts_sources_path%\Hangfire.Mongo\Hangfire.Mongo.csproj /t:Rebuild /t:pack /p:Configuration="Release"

echo copying build output to bin artifacts folder
xcopy /e /v /q /f %build_output% %artifacts_bin_path%

echo moving nuget to own folder in artifacts
move /y %artifacts_bin_path%\*.nupkg %artifacts_nuget_path%

exit /B 0

: UpdateAssemblyInfo
set file=%cd%\artifacts\sources\Hangfire.Mongo\Properties\AssemblyInfo.cs

if not exist "%file%" (
    echo Could not find "%file%"
    exit /B 1
)

if exist "%file%.new" (
    del "%file%.new"
)

for /f "tokens=*" %%a in (%file%) do (
	setlocal enabledelayedexpansion
    set line=%%a
    set assembly_version=!line:AssemblyVersion=!
    set assembly_file_version=!line:AssemblyFileVersion=!
    set assembly_informational_version=!line:AssemblyInformationalVersion=!
    set startChars=!line:~0,2!

    if not !startChars!==// (
        if not !assembly_version!==%%a (
            set "line=[assembly: AssemblyVersion("%build_number%")]"
        ) else if not !assembly_file_version!==%%a (
            set "line=[assembly: AssemblyFileVersion("%build_number%")]"
        ) else if not !assembly_informational_version!==%%a (
            set "line=[assembly: AssemblyInformationalVersion("%build_tag%")]"
        )
    ) 
    echo !line! >> "%file%.new"
	endlocal
)

move "%file%.new" "%file%"
exit /B 0