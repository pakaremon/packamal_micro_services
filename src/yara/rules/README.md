##  System Rules 
These rules are designed to monitor system-level operations and behaviors, focusing on low-level interactions and suspicious file activities.
1. [malware_syscalls.yar](./system/malware_syscalls.yar):
    - Detects common malware-related system calls (e.g., system, execve, popen, CreateProcess, ShellExecute, LoadLibrary, dlopen).
    - Identifies executable files dropped in highly suspicious directories like /tmp/, /var/, /dev/, and /run/.
2. [file_operations.yar](./system/file_operations.yar): Monitors general file system activities.
3. [network_operations.yar](./system/network_operations.yar): Tracks system calls related to network operations.
4. [process_operations.yar](./system/process_operations.yar): Watches for unusual process creation and manipulation.
## Network Rules 
These rules concentrate on identifying suspicious network communication patterns and data exfiltration attempts.
1. [malware_urls.yar](./network/malware_urls.yar):
    - Detects URLs commonly used for malware downloads.
    - Identifies URLs pointing to known executable file extensions (.exe, .sh, .bin, .py, .pl, .php, .js, .jar, .elf).
    - Monitors for files dropped into suspicious directories, often associated with network-based downloads.
2. [data_exfiltration.yar](./network/data_exfiltration.yar): Detects attempts to transfer sensitive data out of the system.
3. [ip_patterns.yar](./network/ip_patterns.yar): Identifies suspicious IP address patterns that might indicate command-and-control communication or malicious servers.
4. [suspicious_domains.yar](./network/suspicious_domains.yar): Flags domain names known to be associated with malicious activity.
## File Rules 
These rules monitor access and operations on the file system, with a focus on sensitive system files and dropped malware.
1. [sensitive_files.yar](./files/sensitive_files.yar):
    - Detects attempts to access critical system files.
    - Monitors access to password files (/etc/passwd, /etc/shadow).
    - Watches for access to sensitive configuration files (/etc/sudoers, /etc/hosts, /etc/ssh/sshd_config).
    - Detects access to log files (/var/log/auth.log, /var/log/secure).
    - Flags access to SSH directories (.ssh/), which often contain sensitive credentials.
2. [dropped_malware.yar](./files/dropped_malware.yar): Identifies and flags malware files that are dropped onto the system by analyzed packages.
## Command Rules 
These rules are designed to detect suspicious command execution patterns, often indicative of post-exploitation activities or malicious script execution.
1. [reverse_shell.yar](./command/reverse_shell.yar): Detects patterns indicative of reverse shell connections, such as nc or netcat commands combined with IP addresses/hostnames and port numbers.
2. [shell_commands.yar](./command/shell_commands.yar): Monitors for the execution of suspicious shell commands, including bash, sh, cmd, powershell, wget, curl, netcat, and various scripting language executions.
2. [execute_tmp.yar](./command/execute_tmp.yar): Specifically detects attempts to execute files from temporary directories, a common malware technique.
3.	[malware_drop.yar](./command/malware_drop.yar): Identifies behaviors associated with malware dropping.
4.	[suspicious_script.yar](./command/suspicious_script.yar): Flags the execution of scripts that exhibit suspicious characteristics.
5.	[system_calls.yar](./command/system_calls.yar): Monitors patterns of system calls, specifically within command execution contexts.
6. [chmod_exec.yar](./command/chmod_exec.yar): Detects instances where chmod +x or similar commands are used to make files executable, often preceding malicious execution.
