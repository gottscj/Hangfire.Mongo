Properties {

	$configuration = if ($env:CONFIGURATION) { $env:CONFIGURATION } else { "Release" };
	$platform = if ($env:PLATFORM) { $env:PLATFORM } else { "AnyCPU" };
	$verbosity = if ($env:VERBOSITY) { $env:VERBOSITY } else { "minimal" };

	# Paths
	$base_path = Resolve-Path ..;

	$build_path = "$base_path\build";
	$sources_path = "$base_path\src"
	$version_file_path = "$sources_path\Hangfire.Mongo\Properties\AssemblyInfo.cs";
	$nuspec_path = "$base_path\nuspecs"

	$artifacts_path = "$base_path\artifacts";
	$artifacts_sources_path = "$base_path\artifacts\sources";
	$artifacts_bin_path = "$base_path\artifacts\bin";
	$artifacts_nuget_path = "$base_path\artifacts\nuget";

	$nuget_exe_path = "$build_path\.nuget\nuget.exe";

	# Supported frameworks
	$target_frameworks = "net45", "netstandard1.5";
	
	# Version
	# Version gathered from Github tag (if it's presented); otherwise it reads from AssemblyInfo.cs file.
	# Assembly version populated into $assemblyVersion property (looks as 1.0.0.1)
	# Package version populated into $packageVersion (looks as 1.0.0-dev1)

	# AppVeyor
	$appVeyor = $env:APPVEYOR;
	$buildNumber = $env:APPVEYOR_BUILD_NUMBER;
	$buildTag = $env:APPVEYOR_REPO_TAG;

}

## Utils

Include "utils\version.ps1";
Include "utils\msbuild.ps1";
Include "utils\dotnet.ps1";

## Build Enviroment

task MsBuild -description "Validates the correct MSBuild version is present" {
	$output = &msbuild /version 2>&1
	assert ($output -like "*14.0*") "MSBuild version 14 required: $output;"
}


## Configuration initialization

Task ValidateConfig -description "Validates configuration" {

	assert('Debug', 'Release' -contains $configuration) "Invalid configuration: $configuration; The variable should contain Debug or Release.";
	assert('AnyCPU' -contains $platform) "Invalid platform: $platform; The variable should contain AnyCPU.";
	assert('quiet', 'minimal', 'normal', 'detailed', 'diagnostic', 'q', 'm', 'n', 'd', 'diag' -contains $verbosity) "Invalid verbosity: $verbosity; The variable should contain quiet, minimal, normal, detailed or diagnostic.";
	assert(Test-Path $nuget_exe_path) "Invalid nuget_exe_path: $nuget_exe_path; Nuget file should exists.";

}

Task GetProjectVersion -description "Initializes version variables to use it during build process" {

	$script:assemblyVersion = Get-AssemblyVersion $version_file_path $buildTag $buildNumber;
	$script:packageVersion = Get-PackageVersion $version_file_path $buildTag $buildNumber;

	Write-Host ("Assembly version: $assemblyVersion") -ForegroundColor White;
	Write-Host ("Package version:  $packageVersion") -ForegroundColor White;

}

Task InitConfiguration -depends ValidateConfig, GetProjectVersion -description "Validates and initializes all configuration variables required for further build process"

## Sources preparation

Task CreateOutputDirs -depends InitConfiguration -description "Creates all necessary folders in ""Artifacts"" folder" {

	if (Test-Path $artifacts_path) {
		Remove-Item $artifacts_path -Recurse -Force;
	}

	New-Item $artifacts_path -Type Directory;
	New-Item $artifacts_sources_path -Type Directory;
	New-Item $artifacts_bin_path -Type Directory;
	New-Item $artifacts_nuget_path -Type Directory;

}

Task PopulateOutputSourcesFolder -depends InitConfiguration, CreateOutputDirs -description "Copy sources folder into ""Artifacts"" folder" {

	Copy-Item "$sources_path\*" $artifacts_sources_path -Recurse;
	Copy-Item "$nuspec_path\*" $artifacts_nuget_path -Recurse;

}

Task UpdateSourcesVersion -depends InitConfiguration, PopulateOutputSourcesFolder -description "Updates version at both AssemblyInfo.cs and and .nuspec" {

	Get-ChildItem $artifacts_sources_path -Recurse -Filter "AssemblyInfo.cs" | foreach { $_.FullName } | Replace-Version -version $assemblyVersion;
	Get-ChildItem $artifacts_nuget_path -Recurse -Filter "*.nuspec" | foreach { $_.FullName } | Replace-Version -version $packageVersion;

}

Task PrepareSources -depends InitConfiguration, CreateOutputDirs, PopulateOutputSourcesFolder, UpdateSourcesVersion -description "Prepares sources ready for further build"

## Build

Task Clean -depends PrepareSources -description "Cleanup project source files" {

	Get-ChildItem $artifacts_sources_path -Recurse -Filter "*.csproj" | Where-Object { $_.DirectoryName -NotLike "*.Sample" } | Clean-Project -config $configuration -platform $platform -verbosity $verbosity;

	Get-ChildItem $artifacts_sources_path -Recurse -Include "project.json" | Where-Object { $_.DirectoryName -NotLike "*.Sample" } | Clean-Project-Dotnet -config $configuration -verbosity $verbosity;
	
}

Task BuildProjects -depends PrepareSources, Clean -description "Build project files and populates bin folder with all required binarines" {

	Get-ChildItem $artifacts_sources_path -Recurse -Filter "*.csproj" | Where-Object { $_.DirectoryName -NotLike "*.Sample" } | Build-Project -config $configuration -platform $platform -outputPath "$artifacts_bin_path\net45" -verbosity $verbosity;

	Get-ChildItem $artifacts_sources_path -Recurse -Include "project.json" | Where-Object { $_.DirectoryName -NotLike "*.Sample" } | Build-Project-Dotnet -config $configuration -outputPath $artifacts_bin_path -verbosity $verbosity;
	
}

Task BuildNugetPackages -depends BuildProjects -description "Build nuget packages" {

	foreach ($framework in $target_frameworks) {
		if (!(Test-Path "$artifacts_nuget_path\$framework\")) {
			New-Item "$artifacts_nuget_path\$framework\" -Type Directory;
		}

		Copy-Item "$artifacts_bin_path\$framework\*" "$artifacts_nuget_path\$framework\" -Recurse;
	}

	$nuspecs = Get-ChildItem $artifacts_nuget_path -Filter "*.nuspec";

	foreach ($nuspec in $nuspecs) {
		exec {
			.$nuget_exe_path pack $nuspec.FullName -OutputDirectory "$artifacts_nuget_path" -BasePath "$artifacts_nuget_path"
		}
	}

}

Task Build -depends PrepareSources, Clean, BuildProjects, BuildNugetPackages -description "Build sources and creating nuget packages"

## Default

Framework "4.6"

Task Default -depends MsBuild, InitConfiguration, PrepareSources, Build;