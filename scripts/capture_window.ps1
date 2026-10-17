# Capture ONE window (cropped to its rectangle, nothing else on screen) to a PNG.
# Usage: .\scripts\capture_window.ps1 -TitleMatch "Clickstream Recs" -Out docs\presentation\shots\shot5.png [-ProcessName chrome]
param([Parameter(Mandatory)][string]$TitleMatch, [Parameter(Mandatory)][string]$Out, [string]$ProcessName = "", [int]$TimeoutSec = 60)
Add-Type -AssemblyName System.Drawing
Add-Type @"
using System; using System.Runtime.InteropServices;
public class W {
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RECT r);
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h, int c);
  [DllImport("user32.dll")] public static extern bool SetProcessDPIAware();
  [StructLayout(LayoutKind.Sequential)] public struct RECT { public int L, T, R, B; }
}
"@
[void][W]::SetProcessDPIAware()
$p = $null
for ($i = 0; $i -lt $TimeoutSec; $i++) {
  $p = Get-Process | Where-Object { $_.MainWindowHandle -ne 0 -and $_.MainWindowTitle -match $TitleMatch -and ($ProcessName -eq "" -or $_.ProcessName -eq $ProcessName) } | Select-Object -First 1
  if ($p) { break }; Start-Sleep 1
}
if (-not $p) { Write-Error "window not found: $TitleMatch"; exit 1 }
[void][W]::ShowWindow($p.MainWindowHandle, 9); [void][W]::SetForegroundWindow($p.MainWindowHandle); Start-Sleep 2
$r = New-Object W+RECT; [void][W]::GetWindowRect($p.MainWindowHandle, [ref]$r)
$w = $r.R - $r.L; $h = $r.B - $r.T
$bmp = New-Object System.Drawing.Bitmap $w, $h
$g = [System.Drawing.Graphics]::FromImage($bmp); $g.CopyFromScreen($r.L, $r.T, 0, 0, $bmp.Size)
$full = Join-Path (Get-Location) $Out
$bmp.Save($full, [System.Drawing.Imaging.ImageFormat]::Png); $g.Dispose(); $bmp.Dispose()
"saved $full ($w x $h) title='$($p.MainWindowTitle)'"
