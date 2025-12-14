rule DataExfiltration: high {
    meta:
        description = "Detect data exfiltration via HTTP POST and GET requests with proper HTTP version headers"
        severity = "HIGH"
        category = "Network Activity"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $data_exfil = /(POST|GET)\s+.*\s+HTTP\/\d\.\d/
    condition:
        $data_exfil
} 