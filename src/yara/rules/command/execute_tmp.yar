rule ExecuteTmp: high {
    meta:
        description = "Detect execution of files from suspicious directories like /tmp, /var, /dev, or /run with command separators"
        severity = "HIGH"
        category = "Command Execution"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $execute_tmp = /(\/tmp\/|\/var\/|\/dev\/|\/run\/)[^\s]+(\s|;|&&|$)/
    condition:
        $execute_tmp
} 