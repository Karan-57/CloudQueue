async function submitJob() {

    const name =
        document.getElementById("jobName").value.trim();

    const type =
        document.getElementById("workloadType").value;


    if (!name) {

        alert("Please enter a workload name");

        return;
    }


    const response = await fetch("/submit", {

        method: "POST",

        headers: {
            "Content-Type": "application/json"
        },

        body: JSON.stringify({

            job_name: name,

            workload_type: type

        })

    });


    const data = await response.json();


    alert(
        "Job submitted successfully!\n" +
        "Job ID: " +
        data.job_id
    );


    document.getElementById("jobName").value = "";

    refreshDashboard();
}


let showAllJobs = false;

function toggleShowAllJobs() {
    showAllJobs = !showAllJobs;
    loadJobs();
}


async function loadJobs() {

    const limit = showAllJobs ? 500 : 100;
    const response = await fetch(`/jobs?limit=${limit}`);

    const jobs = await response.json();

    const container =
        document.getElementById("jobs");

    const toggleContainer =
        document.getElementById("jobsToggleContainer");

    const toggleBtn =
        document.getElementById("toggleJobsBtn");


    if (jobs.length === 0) {

        container.innerHTML =
            "<p>No workloads submitted yet.</p>";

        if (toggleContainer) {
            toggleContainer.style.display = "none";
        }

        return;
    }


    const displayJobs = showAllJobs ? jobs : jobs.slice(0, 10);

    container.innerHTML = displayJobs.map(job => `

        <div class="job">

            <strong>
                #${job.id} - ${job.job_name}
            </strong>

            <br>

            Workload:
            ${job.workload_type}

            <br>

            Status:
            <strong>${job.status}</strong>

            ${job.worker_id ? `<br>Worker: <code>${job.worker_id}</code>` : ""}

            <br>

            Processing:
            ${job.processing_time != null ? job.processing_time + " s" : "-"}

            ${job.waiting_time != null ? `<br>Waiting: ${job.waiting_time} s` : ""}

        </div>

    `).join("");


    if (toggleContainer && toggleBtn) {
        if (jobs.length > 10) {
            toggleContainer.style.display = "block";
            if (showAllJobs) {
                toggleBtn.innerHTML = `<i class="fa-solid fa-chevron-up" style="margin-right: 6px;"></i>Show Less (Top 10)`;
            } else {
                toggleBtn.innerHTML = `<i class="fa-solid fa-chevron-down" style="margin-right: 6px;"></i>View All Workloads (${jobs.length})`;
            }
        } else {
            toggleContainer.style.display = "none";
        }
    }
}


async function loadStats() {

    const response = await fetch("/stats");

    const data = await response.json();


    document.getElementById("total").innerText =
        data.total;

    document.getElementById("completed").innerText =
        data.completed;

    document.getElementById("queued").innerText =
        data.queued;

    document.getElementById("avgTime").innerText =
        data.avg_time + " s";
}


async function loadComparison() {

    const response = await fetch("/comparison");

    const data = await response.json();

    const tbody = document.querySelector("#comparisonTable tbody");

    if (!data || data.length === 0) {
        tbody.innerHTML = `<tr><td colspan="3" class="empty">No completed workloads yet</td></tr>`;
        return;
    }

    tbody.innerHTML = data.map(item => `
        <tr>
            <td><strong>${item.workload_type}</strong></td>
            <td>${item.jobs}</td>
            <td>${item.average_time} s</td>
        </tr>
    `).join("");

}


// ==============================================================================
// Experiment System Frontend Functions
// ==============================================================================

let activeExpInterval = null;
let currentInspectedExpId = null;

async function runExperiment() {
    const workerCount = parseInt(document.getElementById("expWorkers").value);
    const jobCount = parseInt(document.getElementById("expJobs").value);
    const workloadType = document.getElementById("expWorkload").value;
    const runBtn = document.getElementById("runExpBtn");

    if (isNaN(jobCount) || jobCount < 1) {
        alert("Please enter a valid number of jobs (1 or more).");
        return;
    }

    runBtn.disabled = true;
    runBtn.innerHTML = `<i class="fa-solid fa-spinner fa-spin" style="margin-right: 6px;"></i>Starting...`;

    try {
        const response = await fetch("/experiments/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                worker_count: workerCount,
                job_count: jobCount,
                workload_type: workloadType
            })
        });

        const data = await response.json();
        const expId = data.experiment_id;

        // Open live progress box
        const box = document.getElementById("activeExpBox");
        box.style.display = "block";
        document.getElementById("expTitle").innerText = expId;
        document.getElementById("expSubtitle").innerText = `${workerCount} Workers | ${jobCount} Jobs | ${workloadType}`;
        document.getElementById("expStatusBadge").className = "badge-running";
        document.getElementById("expStatusBadge").innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> Running`;
        document.getElementById("expProgressBar").style.width = "0%";
        document.getElementById("expProgressText").innerText = `0 / ${jobCount} completed (0%)`;
        document.getElementById("expElapsedText").innerText = `Elapsed: 0.0 s`;

        document.getElementById("expMetricsGrid").style.display = "none";
        document.getElementById("expWorkerDistSection").style.display = "none";

        // Scroll smoothly to the experiment box
        box.scrollIntoView({ behavior: "smooth", block: "nearest" });

        // Start polling live progress
        if (activeExpInterval) clearInterval(activeExpInterval);
        activeExpInterval = setInterval(() => pollExperiment(expId), 800);

    } catch (err) {
        alert("Error starting experiment: " + err.message);
        runBtn.disabled = false;
        runBtn.innerHTML = `<i class="fa-solid fa-play" style="margin-right: 6px;"></i>Run Experiment`;
    }
}

async function pollExperiment(expId) {
    try {
        const response = await fetch(`/experiments/${expId}`);
        if (!response.ok) return;

        const data = await response.json();

        // Update progress bar & counters
        const pct = data.progress.percentage;
        document.getElementById("expProgressBar").style.width = `${pct}%`;
        document.getElementById("expProgressText").innerText = `${data.progress.completed} / ${data.job_count} completed (${pct}%)`;
        document.getElementById("expElapsedText").innerText = `Elapsed: ${data.elapsed_time} s`;

        if (data.status === "Completed") {
            clearInterval(activeExpInterval);
            activeExpInterval = null;

            const runBtn = document.getElementById("runExpBtn");
            runBtn.disabled = false;
            runBtn.innerHTML = `<i class="fa-solid fa-play" style="margin-right: 6px;"></i>Run Experiment`;

            displayCompletedExperiment(data);
            loadExperimentHistory();
        }
    } catch (e) {
        console.error("Polling error:", e);
    }
}

function displayCompletedExperiment(data) {
    const box = document.getElementById("activeExpBox");
    box.style.display = "block";

    document.getElementById("expTitle").innerText = data.experiment_id;
    document.getElementById("expSubtitle").innerText = `${data.worker_count} Workers | ${data.job_count} Jobs | ${data.workload_type}`;

    document.getElementById("expStatusBadge").className = "badge-completed";
    document.getElementById("expStatusBadge").innerHTML = `<i class="fa-solid fa-circle-check"></i> Completed`;

    document.getElementById("expProgressBar").style.width = "100%";
    document.getElementById("expProgressText").innerText = `${data.progress.completed} / ${data.job_count} completed (100%)`;
    document.getElementById("expElapsedText").innerText = `Total: ${data.total_time} s`;

    // Populate Metrics
    document.getElementById("mTotalTime").innerText = `${data.metrics.total_time} s`;
    document.getElementById("mThroughput").innerText = `${data.metrics.throughput} j/s`;
    document.getElementById("mAvgWait").innerText = `${data.metrics.average_waiting_time} s`;
    document.getElementById("mAvgProc").innerText = `${data.metrics.average_processing_time} s`;
    document.getElementById("mAvgLat").innerText = `${data.metrics.average_total_latency} s`;
    document.getElementById("mCompleted").innerText = data.metrics.completed_jobs;
    document.getElementById("mFailed").innerText = data.metrics.failed_jobs;

    const eff = data.job_count > 0 ? Math.round((data.metrics.completed_jobs / data.job_count) * 100) : 100;
    document.getElementById("mEfficiency").innerText = `${eff}%`;

    document.getElementById("expMetricsGrid").style.display = "grid";

    // Populate worker distribution
    const chipsContainer = document.getElementById("expWorkerChips");
    const dist = data.metrics.worker_distribution || {};
    const workerKeys = Object.keys(dist);

    if (workerKeys.length > 0) {
        chipsContainer.innerHTML = workerKeys.map(k => `
            <span class="worker-chip"><strong>${k}</strong>: ${dist[k]} jobs</span>
        `).join("");
        document.getElementById("expWorkerDistSection").style.display = "block";
    } else {
        document.getElementById("expWorkerDistSection").style.display = "none";
    }
}

let selectedExperimentIds = new Set();
let chartTotalTimeInstance = null;
let chartThroughputInstance = null;
let chartWaitingTimeInstance = null;

function updateSelectedCount() {
    const countBadge = document.getElementById("selectedExpCount");
    if (countBadge) {
        countBadge.innerText = selectedExperimentIds.size;
    }
}

function onExpCheckboxChange(event, expId) {
    event.stopPropagation();
    if (event.target.checked) {
        selectedExperimentIds.add(expId);
    } else {
        selectedExperimentIds.delete(expId);
    }
    updateSelectedCount();

    // Update master selectAll checkbox state
    const allCheckboxes = document.querySelectorAll(".exp-select-checkbox:not(:disabled)");
    const master = document.getElementById("selectAllCheckbox");
    if (master && allCheckboxes.length > 0) {
        master.checked = Array.from(allCheckboxes).every(cb => cb.checked);
    }
}

function toggleSelectAllExp(masterCheckbox) {
    const checkboxes = document.querySelectorAll(".exp-select-checkbox:not(:disabled)");
    checkboxes.forEach(cb => {
        cb.checked = masterCheckbox.checked;
        if (masterCheckbox.checked) {
            selectedExperimentIds.add(cb.value);
        } else {
            selectedExperimentIds.delete(cb.value);
        }
    });
    updateSelectedCount();
}

async function loadExperimentHistory() {
    try {
        const response = await fetch("/experiments");
        const list = await response.json();
        const tbody = document.getElementById("historyTableBody");

        if (!list || list.length === 0) {
            tbody.innerHTML = `<tr><td colspan="9" class="empty">No experiments recorded yet.</td></tr>`;
            return;
        }

        tbody.innerHTML = list.map(exp => `
            <tr class="history-row ${currentInspectedExpId === exp.experiment_id ? 'active' : ''}" onclick="inspectExperiment('${exp.experiment_id}')">
                <td style="text-align: center;" onclick="event.stopPropagation();">
                    <input type="checkbox"
                           class="exp-select-checkbox"
                           value="${exp.experiment_id}"
                           ${selectedExperimentIds.has(exp.experiment_id) ? 'checked' : ''}
                           ${exp.status !== 'Completed' ? 'disabled title="Only completed experiments can be selected"' : ''}
                           onchange="onExpCheckboxChange(event, '${exp.experiment_id}')">
                </td>
                <td><strong>${exp.experiment_id}</strong></td>
                <td>${exp.job_count}</td>
                <td>${exp.worker_count}</td>
                <td>${exp.workload_type}</td>
                <td>${exp.total_time != null ? exp.total_time + ' s' : '-'}</td>
                <td>${exp.throughput != null ? exp.throughput + ' j/s' : '-'}</td>
                <td>
                    <span class="${exp.status === 'Completed' ? 'badge-completed' : (exp.status === 'Running' ? 'badge-running' : 'badge-failed')}">
                        ${exp.status}
                    </span>
                </td>
                <td>
                    <button class="toggle-btn" style="padding: 5px 12px; font-size: 12px;" onclick="event.stopPropagation(); inspectExperiment('${exp.experiment_id}')">
                        <i class="fa-solid fa-chart-line"></i> View
                    </button>
                </td>
            </tr>
        `).join("");

        updateSelectedCount();
    } catch (e) {
        console.error("Error loading experiment history:", e);
    }
}

async function inspectExperiment(expId) {
    currentInspectedExpId = expId;
    try {
        const response = await fetch(`/experiments/${expId}`);
        const data = await response.json();
        displayCompletedExperiment(data);

        // Re-render history table to update active row styling
        loadExperimentHistory();

        const box = document.getElementById("activeExpBox");
        box.scrollIntoView({ behavior: "smooth", block: "nearest" });
    } catch (e) {
        console.error("Error inspecting experiment:", e);
    }
}

async function compareSelectedExperiments() {
    const compSection = document.getElementById("expComparisonSection");
    const errorBanner = document.getElementById("compErrorBanner");
    const errorMsg = document.getElementById("compErrorMessage");
    const validContent = document.getElementById("compValidContent");
    const subtitle = document.getElementById("compSubtitle");

    compSection.style.display = "block";
    compSection.scrollIntoView({ behavior: "smooth", block: "start" });

    if (selectedExperimentIds.size < 2) {
        errorBanner.style.display = "block";
        errorMsg.innerText = selectedExperimentIds.size === 0
            ? "Please select at least two completed experiments using the checkboxes above to compare."
            : "At least two experiments are required for comparison. Please select additional experiments.";
        validContent.style.display = "none";
        subtitle.innerText = "Comparison requires at least two experiments.";
        return;
    }

    try {
        const response = await fetch("/experiments/compare", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                experiment_ids: Array.from(selectedExperimentIds)
            })
        });

        const data = await response.json();

        if (!data.valid) {
            errorBanner.style.display = "block";
            errorMsg.innerText = data.error || "Selected experiments cannot be compared.";
            validContent.style.display = "none";
            subtitle.innerText = "Incompatible experiments selected.";
            return;
        }

        // Valid comparison
        errorBanner.style.display = "none";
        validContent.style.display = "block";
        subtitle.innerText = `Workload: ${data.workload_type} | Workload Size: ${data.job_count} jobs across ${data.experiments.length} configurations`;

        renderComparisonTable(data.experiments);
        renderComparisonCharts(data.experiments);

    } catch (err) {
        errorBanner.style.display = "block";
        errorMsg.innerText = "Error generating comparison: " + err.message;
        validContent.style.display = "none";
    }
}

function renderComparisonTable(experiments) {
    const headRow = document.getElementById("compTableHeadRow");
    const tbody = document.getElementById("compTableBody");

    // Build header
    headRow.innerHTML = `
        <th>Metric</th>
        ${experiments.map(e => `
            <th>
                ${e.worker_count} Worker${e.worker_count > 1 ? 's' : ''}
                <div style="font-weight: normal; font-size: 11px; color: #64748b; margin-top: 2px;">
                    ${e.experiment_id}
                </div>
            </th>
        `).join("")}
    `;

    // Define metrics rows
    const metrics = [
        { label: "Total Wall-Clock Time", key: "total_time", unit: "s", lowerBetter: true },
        { label: "Throughput", key: "throughput", unit: "jobs/s", lowerBetter: false },
        { label: "Average Waiting Time", key: "average_waiting_time", unit: "s", lowerBetter: true },
        { label: "Average Processing Time", key: "average_processing_time", unit: "s", lowerBetter: true },
        { label: "Average Total Latency", key: "average_total_latency", unit: "s", lowerBetter: true },
        { label: "Jobs Completed / Total", key: "jobs_display", unit: "", isJobs: true }
    ];

    tbody.innerHTML = metrics.map(m => {
        return `
            <tr>
                <td><strong>${m.label}</strong></td>
                ${experiments.map(e => {
                    if (m.isJobs) {
                        return `<td>${e.completed_jobs} / ${e.job_count}</td>`;
                    }
                    const val = e[m.key] != null ? `${e[m.key]} ${m.unit}` : "-";
                    return `<td><strong>${val}</strong></td>`;
                }).join("")}
            </tr>
        `;
    }).join("");
}

function renderComparisonCharts(experiments) {
    if (typeof Chart === "undefined") {
        console.warn("Chart.js is not loaded.");
        return;
    }

    const labels = experiments.map(e => `${e.worker_count} Worker${e.worker_count > 1 ? 's' : ''}`);

    // Chart 1: Total Time vs Workers
    const ctxTime = document.getElementById("chartTotalTime").getContext("2d");
    if (chartTotalTimeInstance) chartTotalTimeInstance.destroy();
    chartTotalTimeInstance = new Chart(ctxTime, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [{
                label: "Total Time (seconds)",
                data: experiments.map(e => e.total_time),
                backgroundColor: "rgba(239, 68, 68, 0.75)",
                borderColor: "#dc2626",
                borderWidth: 1.5,
                borderRadius: 5
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: "Seconds" }
                },
                x: {
                    title: { display: true, text: "Worker Capacity" }
                }
            }
        }
    });

    // Chart 2: Throughput vs Workers
    const ctxThroughput = document.getElementById("chartThroughput").getContext("2d");
    if (chartThroughputInstance) chartThroughputInstance.destroy();
    chartThroughputInstance = new Chart(ctxThroughput, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [{
                label: "Throughput (jobs/sec)",
                data: experiments.map(e => e.throughput),
                backgroundColor: "rgba(16, 185, 129, 0.75)",
                borderColor: "#059669",
                borderWidth: 1.5,
                borderRadius: 5
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: "Jobs / Sec" }
                },
                x: {
                    title: { display: true, text: "Worker Capacity" }
                }
            }
        }
    });

    // Chart 3: Waiting Time vs Workers
    const ctxWait = document.getElementById("chartWaitingTime").getContext("2d");
    if (chartWaitingTimeInstance) chartWaitingTimeInstance.destroy();
    chartWaitingTimeInstance = new Chart(ctxWait, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [{
                label: "Avg Waiting Time (seconds)",
                data: experiments.map(e => e.average_waiting_time),
                backgroundColor: "rgba(245, 158, 11, 0.75)",
                borderColor: "#d97706",
                borderWidth: 1.5,
                borderRadius: 5
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: "Seconds" }
                },
                x: {
                    title: { display: true, text: "Worker Capacity" }
                }
            }
        }
    });
}

function closeComparison() {
    const compSection = document.getElementById("expComparisonSection");
    if (compSection) {
        compSection.style.display = "none";
    }
}


async function refreshDashboard() {

    await loadStats();

    await loadJobs();

}


refreshDashboard();

loadExperimentHistory();

setInterval(refreshDashboard, 5000);