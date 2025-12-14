rule SuspiciousDomains: high {
    meta:
        description = "Detect suspicious domains commonly used for command and control, data exfiltration, and security testing tools"
        severity = "HIGH"
        category = "Network Activity"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $suspicious_domains = /(burpcollaborator\.net|pipedream\.com|interact\.sh|ngrok\.io|serveo\.net|requestbin\.net|canarytokens\.com)/
    condition:
        $suspicious_domains
} 