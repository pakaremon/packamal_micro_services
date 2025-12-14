function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

function showLoader() {
    const loader = document.getElementById('loader');
    if (loader) loader.classList.remove('d-none');
}

function hideLoader() {
    const loader = document.getElementById('loader');
    if (loader) loader.classList.add('d-none');
}

function submitForm() {
    const loader = document.getElementById('loader');
    showLoader();

    const form = document.getElementById('submitForm');
    const formData = new FormData(form);

    fetch(form.action, {
        method: 'POST',
        body: formData,
        headers: {
            'X-CSRFToken': getCookie('csrftoken')
        }
    })
        .then(response => response.json())
        .then(data => {
            console.log('Success:', data);
            if (form.action.includes('bandit4mal')) {
                displayBanditReport(data.bandit4mal_report);
            } else if (form.action.includes('lastpymile')) {
                displayLastbyMileReport(data.lastpymile_report);
            } else {
                displayReport(data);
            }
        })
        .catch((error) => {
            console.error('Error:', error);
            alert('An error occurred. Please try again.');
        })
        .finally(() => {
            if (!form.action.includes('dynamic_analysis')) {
                hideLoader();
            }
        });
}

function submitDynamicAnalysisForm() {
    const loader = document.getElementById('loader');
    showLoader();

    const form = document.getElementById('submitForm');
    const formData = new FormData(form);

    fetch(form.action, {
        method: 'POST',
        body: formData,
        headers: {
            'X-CSRFToken': getCookie('csrftoken')
        }
    })
        .then(response => response.json())
        .then(data => {
            if (data.task_id) {
                console.log('Task submitted, polling for status:', data.task_id);
                pollTaskStatus(data.task_id);
            } else {
                console.error('No task_id returned');
                hideLoader();
                alert('Failed to submit analysis task.');
            }
        })
        .catch((error) => {
            console.error('Error:', error);
            hideLoader();
            alert('An error occurred during submission.');
        });
}

function pollTaskStatus(taskId) {
    const pollInterval = 2000; // 2 seconds

    const checkStatus = () => {
        fetch(`/api/task/${taskId}/status/`)
            .then(response => response.json())
            .then(data => {
                console.log('Task status:', data.status);
                if (data.status === 'completed') {
                    displayDynamicReport(data.result);
                    hideLoader();
                } else if (data.status === 'failed') {
                    console.error('Task failed:', data.result);
                    hideLoader();
                    alert('Analysis failed: ' + (data.result || 'Unknown error'));
                } else {
                    // Still pending or processing, poll again
                    setTimeout(checkStatus, pollInterval);
                }
            })
            .catch(error => {
                console.error('Error polling task status:', error);
                hideLoader();
            });
    };

    checkStatus();
}

function displayBanditReport(data) {
    var feedbackPanel = document.getElementById('feedback-panel');
    feedbackPanel.classList.remove('d-none');

    if (!data || !data.results) {
        console.error("Invalid Bandit report data");
        return;
    }

    const fileAlertCounts = data.results.reduce((acc, result) => {
        const key = result.filename;
        if (!acc[key]) {
            acc[key] = { HIGH: 0, MEDIUM: 0, LOW: 0 };
        }
        acc[key][result.issue_severity] = (acc[key][result.issue_severity] || 0) + 1;
        return acc;
    }, {});

    if (data.metrics && data.metrics._totals) {
        document.getElementById('critical-alerts').textContent = data.metrics._totals['SEVERITY.HIGH'] || 0;
        document.getElementById('high-alerts').textContent = data.metrics._totals['SEVERITY.MEDIUM'] || 0;
        document.getElementById('medium-alerts').textContent = data.metrics._totals['SEVERITY.LOW'] || 0;
    }

    const fileAlertsTable = document.getElementById('file-alerts');
    if (fileAlertsTable) {
        fileAlertsTable.innerHTML = Object.entries(fileAlertCounts).map(([filename, counts]) => `
            <tr>
                <td>${filename}</td>
                <td>${counts.HIGH}</td>
                <td>${counts.MEDIUM}</td>
                <td>${counts.LOW}</td>
            </tr>
        `).join('');
    }
}

function displayLastbyMileReport(data) {
    const feedbackPanel = document.getElementById('feedback-panel');
    feedbackPanel.classList.remove('d-none');

    let reportHTML = `
    <div class="card shadow-lg">
        <div class="card-header bg-primary text-white">
            <h1 class="card-title">Report</h1>
        </div>
        <div class="card-body">
            <h2 class="h5">Package Name: ${data.package.name}</h2>
            <h2 class="h5">Package Version: ${data.package.version}</h2>
            <h2 class="h5">Analysis Date: ${data.date}</h2>
            <h2 class="h5">Duration: ${data.duration_ms} ms</h2>
            <h2 class="h5">Status: ${data.completed ? "Completed" : "Incomplete"}</h2>
        </div>
    </div>
    `;

    if (data.results) {
        data.results.forEach(result => {
            reportHTML += `
            <div class="mt-4">
                <h3>Release: ${result.release}</h3>
                <p>A phantom is a file or line of code in an artifact that doesn't match what's in the source repository.</p>
            </div>
            `;

            const risks = ['phantom_files', 'low_risk_files', 'medium_risk_files', 'high_risk_files'];
            risks.forEach(risk => {
                if (result[risk] && result[risk].length > 0) {
                    reportHTML += `
                    <div class="mt-4">
                        <h4>${risk.replace('_', ' ').toUpperCase()}</h4>
                        <table class="table table-bordered table-striped">
                            <thead>
                                <tr>
                                    <th>File</th>
                                    <th>Bandit4mal Alerts</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;
                    result[risk].forEach(file => {
                        const banditAlertsCount = file.bandit_report
                            ? file.bandit_report.filter(alert => alert.issue_severity === 'MEDIUM' || alert.issue_severity === 'HIGH').length
                            : 0;
                        reportHTML += `
                        <tr>
                            <td>${file.file}</td>
                            <td>${banditAlertsCount}</td>
                        </tr>
                        `;
                    });
                    reportHTML += `
                            </tbody>
                        </table>
                    </div>
                    `;
                } else {
                    reportHTML += `
                    <div class="mt-4">
                        <h4>${risk.replace('_', ' ').toUpperCase()}</h4>
                        <p>No files found</p>
                    </div>
                    `;
                }
            });
        });
    }

    if (data.statistics && data.statistics.length > 0) {
        reportHTML += `
        <div class="mt-4">
            <h3>Statistics</h3>
            <table class="table table-bordered table-striped">
                <thead>
                    <tr>
                        <th>Stage Name</th>
                        <th>Duration (ms)</th>
                        <th>Git Repository</th>
                        <th>Processed Commits</th>
                        <th>Processed Files</th>
                    </tr>
                </thead>
                <tbody>
        `;
        data.statistics.forEach(stat => {
            reportHTML += `
            <tr>
                <td>${stat.stage_name}</td>
                <td>${stat.duration_ms}</td>
                <td>${stat.git_repository || "N/A"}</td>
                <td>${stat.processed_commits || "N/A"}</td>
                <td>${stat.processed_files}</td>
            </tr>
            `;
        });
        reportHTML += `
                </tbody>
            </table>
        </div>
        `;
    }

    feedbackPanel.innerHTML = reportHTML;
}

function displayReport(data) {
    const feedbackPanel = document.getElementById('feedback-panel');
    feedbackPanel.classList.remove('d-none');

    if (data.typosquatting_candidates) {
        const typosquattingCandidates = data.typosquatting_candidates.map(candidate => candidate || []).flat();
        feedbackPanel.innerHTML = `
            <div class="report-details card shadow-lg">
                <div class="card-header bg-primary text-white">
                    <h1 class="card-title">Report Detail</h1>
                </div>
                <div class="card-body">
                    <h2 class="h5">Typosquatting Candidates</h2>
                    <ul class="list-group mb-3">
                        ${typosquattingCandidates.length > 0
                ? typosquattingCandidates.map(candidate => `<li class="list-group-item">${candidate}</li>`).join('')
                : '<li class="list-group-item">No typosquatting candidates found</li>'}
                    </ul>
                </div>
            </div>
        `;
    }

    if (data.source_urls) {
        const sources = data.source_urls.map(source => source || []).flat();
        feedbackPanel.innerHTML = `
            <div class="report-details card shadow-lg">
                <div class="card-header bg-primary text-white">
                    <h1 class="card-title">Report</h1>
                </div>
                <div class="card-body">
                    <h2 class="h5">Source URLs</h2>
                    <ul class="list-group mb-3">
                        ${sources.length > 0
                ? sources.map(source => `<li class="list-group-item">${source}</li>`).join('')
                : '<li class="list-group-item">No source URLs found</li>'}
                    </ul>
                </div>
            </div>
        `;
    }

    if (data.package && data.results) {
        let reportHTML = `
            <div class="report-details card shadow-lg">
            <div class="card-header bg-primary text-white">
                <h1 class="card-title">Report Detail</h1>
            </div>
            <div class="card-body">
               <h2 class="h5">Package Name: ${data.package.name}</h2>
                <h2 class="h5">Package Version: ${data.package.version}</h2>
                <h2>Files Modified in Releases Flagged by Bandit Tool</h2>`;

        data.results.forEach(dt => {
            reportHTML += `<h3>Release: ${dt.release}</h3>`;
            const risks = ['phantom_files', 'low_risk_files', 'medium_risk_files', 'high_risk_files'];
            risks.forEach(risk => {
                if (dt[risk] && dt[risk].length > 0) {
                    reportHTML += `<h4>${risk.replace('_', ' ').toUpperCase()}</h4><ul>`;
                    dt[risk].forEach(item => {
                        reportHTML += `<li><strong>File:</strong> ${item.file}<br>`;
                        reportHTML += `<strong>File Hash:</strong> ${item.file_hash}<br>`;
                        reportHTML += `<strong>Bandit Report:</strong><ul>`;
                        item.bandit_report.forEach(report => {
                            reportHTML += `<li><strong>Issue:</strong> ${report.issue_text}<br>`;
                            reportHTML += `<strong>Severity:</strong> ${report.issue_severity}<br>`;
                            reportHTML += `<strong>Confidence:</strong> ${report.issue_confidence}<br>`;
                            reportHTML += `<strong>Line Number:</strong> ${report.line_number}<br>`;
                            reportHTML += `<strong>Code:</strong> <pre>${report.code}</pre></li>`;
                        });
                        reportHTML += `</ul></li>`;
                    });
                    reportHTML += `</ul>`;

                } else {
                    reportHTML += `<h4>${risk.replace('_', ' ').toUpperCase()}</h4><p>No files found</p>`;
                }
            });
        });

        reportHTML += `</div>
                </div>`;

        feedbackPanel.innerHTML = reportHTML;
    }
}

function displayDynamicReport(data) {
    const feedbackPanel = document.getElementById('feedback-panel');
    feedbackPanel.classList.remove('d-none');

    // Combine install and execute phases
    const combinedReport = {
        num_files: data.dynamic_analysis_report.install.num_files + data.dynamic_analysis_report.execute.num_files,
        num_commands: data.dynamic_analysis_report.install.num_commands + data.dynamic_analysis_report.execute.num_commands,
        num_network_connections: data.dynamic_analysis_report.install.num_network_connections + data.dynamic_analysis_report.execute.num_network_connections,
        num_system_calls: data.dynamic_analysis_report.install.num_system_calls + data.dynamic_analysis_report.execute.num_system_calls,
        files: {
            read: [...new Set([...data.dynamic_analysis_report.install.files.read, ...data.dynamic_analysis_report.execute.files.read])],
            write: [...new Set([...data.dynamic_analysis_report.install.files.write, ...data.dynamic_analysis_report.execute.files.write])],
            delete: [...new Set([...data.dynamic_analysis_report.install.files.delete, ...data.dynamic_analysis_report.execute.files.delete])]
        },
        dns: [...new Set([...data.dynamic_analysis_report.install.dns, ...data.dynamic_analysis_report.execute.dns])],
        ips: [...new Map([...data.dynamic_analysis_report.install.ips, ...data.dynamic_analysis_report.execute.ips].map(ip => [JSON.stringify(ip), ip])).values()],
        commands: [...new Set([...data.dynamic_analysis_report.install.commands, ...data.dynamic_analysis_report.execute.commands])],
        syscalls: Object.entries(
            [...data.dynamic_analysis_report.install.syscalls, ...data.dynamic_analysis_report.execute.syscalls]
                .reduce((acc, syscall) => {
                    acc[syscall] = (acc[syscall] || 0) + 1;
                    return acc;
                }, {})
        )
            .sort((a, b) => b[1] - a[1]) // Sort by occurrence in descending order
            .map(([syscall]) => syscall), // Extract only the syscall names,
        yara: data.dynamic_analysis_report.yara_analysis
    };

    populateDynamicSections(combinedReport);
}

function populateDynamicSections(combinedReport) {
    // Populate Yara Section
    const yaraContent = document.getElementById('yara-content');
    if (yaraContent) {
        yaraContent.innerHTML = '';
        const allYaraMatches = [
            ...(combinedReport.yara.command_matches || []),
            ...(combinedReport.yara.network_matches || []),
            ...(combinedReport.yara.syscall_matches || []),
            ...(combinedReport.yara.files_matches || []),
        ];

        allYaraMatches.forEach((match, index) => {
            yaraContent.innerHTML += `
                <tr>
                    <td>${index + 1}</td> 
                    <td>${match.rule}</td>
                    <td>${match.category}</td>
                    <td>${match.description}</td>
                    <td>${match.severity}</td>
                    <td>${match.evidence.join(', ')}</td>
                    <td><a href="${match.url}" target="_blank">${match.url}</a></td>
                </tr>
            `;
        });
    }

    // Populate Files
    ['read', 'write', 'delete'].forEach(type => {
        const filesContent = document.getElementById('files-content');
        if (filesContent) {
            filesContent.innerHTML += `
                <tr>
                    <td colspan="2"><strong>${type.charAt(0).toUpperCase() + type.slice(1)} Files</strong></td>
                </tr>
            `;
            combinedReport.files[type].slice(0, 100).forEach((path, index) => {
                filesContent.innerHTML += `
                    <tr>
                        <td>${index + 1}</td>
                        <td>${path}</td>
                    </tr>
                `;
            });
        }
    });

    // Populate DNS Queries
    const dnsContent = document.getElementById('dns-content');
    if (dnsContent) {
        combinedReport.dns.forEach((hostname, index) => {
            dnsContent.innerHTML += `
                <tr>
                    <td>${index + 1}</td>
                    <td>${hostname}</td>
                </tr>
            `;
        });
    }

    // Populate IP Connections
    const ipContent = document.getElementById('ip-content');
    if (ipContent) {
        combinedReport.ips.forEach((ip, index) => {
            ipContent.innerHTML += `
                <tr>
                    <td>${index + 1}</td>
                    <td>${ip.Address}</td>
                    <td>${ip.Port}</td>
                    <td>${ip.Hostnames || 'N/A'}</td>
                </tr>
            `;
        });
    }

    // Populate Commands
    const commandsContent = document.getElementById('commands-content');
    if (commandsContent) {
        combinedReport.commands.forEach((command, index) => {
            commandsContent.innerHTML += `
                <tr>
                    <td>${index + 1}</td>
                    <td>${command}</td>
                </tr>
            `;
        });
    }

    // Populate Syscalls
    const syscallsContent = document.getElementById('syscalls-content');
    if (syscallsContent) {
        combinedReport.syscalls.slice(0, 500).forEach((syscall, index) => {
            syscallsContent.innerHTML += `
                <tr>
                    <td>${index + 1}</td>
                    <td>${syscall}</td>
                </tr>
            `;
        });
    }
}