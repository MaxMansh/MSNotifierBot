Set WshShell = CreateObject("WScript.Shell")

scriptPath = WScript.ScriptFullName
projectDir = Left(scriptPath, InStrRev(scriptPath, "\") - 1)

WshShell.Run "cmd /c """ & projectDir & "\run_script.bat""", 0, False