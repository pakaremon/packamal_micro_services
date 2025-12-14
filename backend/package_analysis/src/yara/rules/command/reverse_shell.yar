rule ReverseShell: high {
    meta:
        description = "Detect reverse shell connection patterns using netcat or nc with IP addresses or hostnames and port numbers"
        severity = "HIGH"
        category = "Command Execution"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $reverse_shell = /(nc|netcat)\s+.*\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[a-zA-Z0-9.-]+)\s+\d+/
    condition:
        $reverse_shell
} 