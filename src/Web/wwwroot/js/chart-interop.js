window.nexusDataSpaceCharts = (function () {
    const instances = {};
    let zoomRegistered = false;

    function ensureZoomRegistered() {
        if (zoomRegistered) return;
        if (window.ChartZoom && window.Chart) {
            window.Chart.register(window.ChartZoom);
            zoomRegistered = true;
        } else if (window.Chart && window.Chart.registry && window.chartjsPluginZoom) {
            window.Chart.register(window.chartjsPluginZoom);
            zoomRegistered = true;
        }
    }

    function destroy(id) {
        if (instances[id]) {
            instances[id].destroy();
            delete instances[id];
        }
    }

    function resetZoom(id) {
        const c = instances[id];
        if (c && typeof c.resetZoom === 'function') c.resetZoom();
    }

    function formatLabel(v) {
        if (v === null || v === undefined) return '';
        const s = String(v);
        const m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$/);
        if (m) return `${m[3]}.${m[2]}.${m[1].slice(2)}`;
        return s;
    }

    function render(id, type, labels, values, title) {
        const el = document.getElementById(id);
        if (!el) return;
        ensureZoomRegistered();
        destroy(id);

        const palette = [
            '#3B82F6', '#22D3EE', '#6366F1', '#00D1FF', '#38BDF8',
            '#22C55E', '#F59E0B', '#EF4444', '#06B6D4', '#2563EB'
        ];

        const displayLabels = labels.map(formatLabel);
        const n = displayLabels.length;
        const longest = displayLabels.reduce((a, b) => (b && b.length > a ? b.length : a), 0);
        const needsRotation = n > 6 || longest > 8;

        const backgroundColors = type === 'pie'
            ? displayLabels.map((_, i) => palette[i % palette.length])
            : (ctx) => {
                const chart = ctx.chart;
                const area = chart.chartArea;
                if (!area) return '#3B82F6';
                const gradient = chart.ctx.createLinearGradient(0, area.bottom, 0, area.top);
                gradient.addColorStop(0, '#3B82F6');
                gradient.addColorStop(1, '#00D1FF');
                return gradient;
            };

        const data = {
            labels: displayLabels,
            datasets: [{
                label: title || '',
                data: values,
                backgroundColor: backgroundColors,
                borderColor: type === 'line' ? '#22D3EE' : '#3B82F6',
                borderWidth: type === 'line' ? 2 : 1,
                borderRadius: type === 'bar' ? 4 : 0,
                pointRadius: type === 'line' ? 3 : 0,
                pointHoverRadius: 5,
                pointHitRadius: type === 'line' ? 14 : 0,
                tension: 0.3,
                fill: type === 'line' ? { target: 'origin', above: 'rgba(59,130,246,0.12)' } : true
            }]
        };

        const xAxis = {
            ticks: {
                autoSkip: true,
                maxRotation: needsRotation ? 55 : 0,
                minRotation: needsRotation ? 35 : 0,
                maxTicksLimit: Math.min(n, 24),
                font: { size: 11 },
                color: '#94A3B8'
            },
            grid: { color: 'rgba(255,255,255,0.05)' },
            border: { color: 'rgba(255,255,255,0.08)' }
        };

        const yAxis = {
            beginAtZero: true,
            ticks: {
                font: { size: 11 },
                color: '#94A3B8',
                callback: function (v) {
                    if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toFixed(1) + 'M';
                    if (Math.abs(v) >= 1_000) return (v / 1_000).toFixed(0) + 'k';
                    return v;
                }
            },
            grid: { color: 'rgba(255,255,255,0.05)' },
            border: { display: false }
        };

        const isZoomable = type !== 'pie';
        const plugins = {
            legend: { display: type === 'pie', position: 'right', labels: { boxWidth: 12, font: { size: 11 }, color: '#E2E8F0' } },
            title: { display: !!title, text: title, font: { size: 13, weight: '600' }, color: '#F8FAFC', padding: { bottom: 10 } },
            tooltip: {
                backgroundColor: '#020617',
                borderColor: 'rgba(59, 130, 246, 0.3)',
                borderWidth: 1,
                titleColor: '#F8FAFC',
                bodyColor: '#E2E8F0',
                titleFont: { size: 12, weight: '600' },
                bodyFont: { size: 12 },
                padding: 10,
                displayColors: type === 'pie'
            }
        };

        if (isZoomable && zoomRegistered) {
            plugins.zoom = {
                limits: {
                    x: { min: 'original', max: 'original', minRange: 1 },
                    y: { min: 'original', max: 'original' }
                },
                pan: {
                    enabled: true,
                    mode: 'xy',
                    modifierKey: null
                },
                zoom: {
                    wheel: { enabled: true, speed: 0.1, modifierKey: null },
                    pinch: { enabled: true },
                    drag: {
                        enabled: true,
                        modifierKey: 'shift',
                        backgroundColor: 'rgba(59,130,246,0.15)',
                        borderColor: 'rgba(0,209,255,0.6)',
                        borderWidth: 1
                    },
                    mode: 'x'
                }
            };
        }

        const cfg = {
            type: type === 'line' ? 'line' : type === 'pie' ? 'pie' : 'bar',
            data: data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { top: 8, right: 12, bottom: 4, left: 4 } },
                interaction: {
                    mode: 'nearest',
                    intersect: true
                },
                hover: {
                    mode: 'nearest',
                    intersect: true
                },
                plugins: plugins,
                scales: type === 'pie' ? {} : { x: xAxis, y: yAxis }
            }
        };

        instances[id] = new Chart(el.getContext('2d'), cfg);
    }

    return { render, destroy, resetZoom };
})();

window.nexusDataSpaceChat = {
    scrollToBottom(id) {
        const el = document.getElementById(id);
        if (el) {
            requestAnimationFrame(() => {
                el.scrollTop = el.scrollHeight;
            });
        }
    }
};
