rule ShellCommandExecution: high {
    meta:
        description = "Detect suspicious shell command execution patterns including bash, sh, cmd, powershell, wget, curl, netcat, and scripting languages"
        severity = "HIGH"
        category = "Command Execution"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $shell_commands = /(bash|sh|cmd|powershell|wget|curl|nc|netcat|python|perl|ruby|php)\s+/
    condition:
        $shell_commands
} 