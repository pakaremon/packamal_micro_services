rule SensitiveFileAccess: high {
    meta:
        description = "Detect attempts to read sensitive system files including password files, configuration files, logs, and SSH directories"
        severity = "HIGH"
        category = "File Operations"
        author = "PackageGuard Team"
        date = "2025"
    strings:
        $passwd_file = /\/etc\/passwd/
        $shadow_file = /\/etc\/shadow/
        $group_file = /\/etc\/group/
        $sudoers_file = /\/etc\/sudoers/
        $hosts_file = /\/etc\/hosts/
        $hostname_file = /\/etc\/hostname/
        $auth_log = /\/var\/log\/auth\.log/
        $secure_log = /\/var\/log\/secure/
        $sshd_config = /\/etc\/ssh\/sshd_config/
        $fstab_file = /\/etc\/fstab/
        $grub_cfg = /\/boot\/grub\/grub\.cfg/
        $pam_d_dir = /\/etc\/pam\.d/
        $ssh_dir = /\.ssh\//
    condition:
        any of ($passwd_file, $shadow_file, $group_file, $sudoers_file, $hosts_file, 
                $hostname_file, $auth_log, $secure_log, $sshd_config, $fstab_file, 
                $grub_cfg, $pam_d_dir, $ssh_dir)
} 