rule SystemCallExecution: high {
    meta:
        description = "Detect suspicious system call execution patterns including system, exec, eval, spawn, fork, and popen functions"
        severity = "HIGH"
        category = "Command Execution"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $system_calls = /(system|exec|eval|spawn|fork|popen)/
    condition:
        $system_calls
} 