rule NetworkOperations: high {
    meta:
        description = "Detect suspicious network operations including connect, bind, listen, and accept system calls"
        severity = "HIGH"
        category = "System Calls"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $network_ops = /(connect|bind|listen|accept)/
    condition:
        $network_ops
} 