function Get-Version($path, $buildTag) {
	<#
	.SYNOPSIS
	Return version from github repo tag or assembly version file.
	.DESCRIPTION
	The Get-Version function uses git repo tag to determine version. If it isn't set it uses regular expressions to process .cs file and return content of [AssemblyVersion] attribute.
	.PARAMETER path
	Path to version information file.
	#>

	$result = $null;

	if ($buildTag -match "^v\d+\.{1}\d+\.{1}\d+$")
	{
		$result = [regex]::match($buildTag, "^v(?<version>\d+\.{1}\d+\.{1}\d+)$").Groups["version"];
		
		if ($version.Success -and ($version.Value -match "^\d+\.{1}\d+\.{1}\d+$"))
		{
			$result = $version.Value;
		}
	}
	else
	{
		$fileType = [System.IO.Path]::GetExtension($path);
	
		if ($fileType -eq ".cs")
		{
			$line = (((Get-Content $path) -match "(AssemblyVersion)\s*\(\""(.+?)(\""\))") | where {$_ -notlike "//*"});
			$version = [regex]::match($line, "(AssemblyVersion)\s*\(\""(?<version>.+?)(\""\))").Groups["version"];

			if ($version.Success -and ($version.Value -match "^\d+\.{1}\d+\.{1}\d+$"))
			{
				$result = $version.Value;
			}
		}
		else {
			throw [System.ArgumentException] """$fileType"" is wrong extension.";
		}
	}

	return $result;
}

function Get-AssemblyVersion($path, $buildTag, $buildNumber) {
	<#
	.SYNOPSIS
	Return assembly version.
	.DESCRIPTION
	Uses Get-Version and adds build number.
	.PARAMETER path
	Path to version information file.
	.PARAMETER buildTag
	Github repo tag
	.PARAMETER buildNumber
	CI build number
	#>

	$result = Get-Version $path $buildTag;

	if ($buildNumber)
	{
		$result = "$result.$buildNumber";
	}
	else
	{
		$result = "$result.0";
	}

	return $result;
}

function Get-PackageVersion($path, $buildTag, $buildNumber) {
	<#
	.SYNOPSIS
	Return package version in semver format.
	.DESCRIPTION
	Uses Get-Version and adds build number.
	.PARAMETER path
	Path to version information file.
	.PARAMETER buildTag
	Github repo tag
	.PARAMETER buildNumber
	CI build number
	#>
	
	$result = Get-Version $path $buildTag;

	if ($buildTag)
	{
		$result = $result;
	}
	elseif ($buildNumber)
	{
		$result = "$result-dev$buildNumber";
	}
	else
	{
		$result = "$result-dev0";
	}

	return $result;
}


function Replace-Version {
	<#
	.SYNOPSIS
	Update assembly version attributes with actual version.
	.DESCRIPTION
	The Replace-Version function uses regular expressions to process .cs file and replace all "Assembly___Version(...)" attributes with actucal version.
	.PARAMETER version
	Actual version which should be placed into attributes.
	#>
	[CmdletBinding()]
	param(
		[Parameter(Mandatory=$True, ValueFromPipeline=$True)]
		[string[]]$path,
		[Parameter()]
		[string]$version
	)

	PROCESS {

		$fileType = [System.IO.Path]::GetExtension($path);

		if ($fileType -eq ".cs")
		{
			(Get-Content $path) -replace "(Assembly\w*Version)\s*\(\"".+?(\""\))", "`$1`(""$version"")" | Set-Content $path;
		}
		elseif ($fileType -eq ".nuspec")
		{
			$xml = [xml](Get-Content $path);
			$xml.package.metadata.version = $version;
			$xml.Save($path);
		}
		else {
			throw [System.ArgumentException] """$fileType"" is wrong extension.";
		}

	}
}