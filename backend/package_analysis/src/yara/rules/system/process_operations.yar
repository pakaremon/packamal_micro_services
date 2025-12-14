rule ProcessOperations: high {
    meta:
        description = "Detect suspicious process operations including fork, exec, spawn, and kill system calls"
        severity = "HIGH"
        category = "System Calls"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $process_ops = /(fork|exec|spawn|kill)/
    condition:
        $process_ops
} 