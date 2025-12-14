rule IPPatterns: high {
    meta:
        description = "Detect IPv4 address patterns in network communications for potential command and control or data exfiltration"
        severity = "HIGH"
        category = "Network Activity"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $ip_pattern = /((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)/
    condition:
        $ip_pattern
} 