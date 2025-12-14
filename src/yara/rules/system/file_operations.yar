rule FileOperations: high {
    meta:
        description = "Detect suspicious file operations including open, write, read, delete, remove, and unlink system calls"
        severity = "HIGH"
        category = "System Calls"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $file_ops = /(open|write|read|delete|remove|unlink)/
    condition:
        $file_ops
} 