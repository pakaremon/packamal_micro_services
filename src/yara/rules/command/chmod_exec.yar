rule ChmodExecution: high {
    meta:
        description = "Detect chmod execution on suspicious directories to make files executable in /tmp, /var, /dev, or /run locations"
        severity = "HIGH"
        category = "Command Execution"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $chmod_exec = /chmod\s+\+x\s+(\/tmp\/|\/var\/|\/dev\/|\/run\/)[^\s]+/
    condition:
        $chmod_exec
} 