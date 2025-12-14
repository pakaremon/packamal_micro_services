rule SuspiciousScript: high {
    meta:
        description = "Detect suspicious script creation and execution patterns using echo/printf with base64, uudecode, gunzip, tar, unzip to suspicious directories"
        severity = "HIGH"
        category = "Command Execution"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $suspicious_script = /(echo|printf)\s+.*(base64|uudecode|gunzip|tar|unzip).*(>|>>|\|)\s*(\/tmp\/|\/var\/|\/dev\/|\/run\/)[^\s]+/
    condition:
        $suspicious_script
} 