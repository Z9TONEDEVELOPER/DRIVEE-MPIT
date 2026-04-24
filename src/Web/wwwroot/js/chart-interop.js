window.driveeCharts = (function () {
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
            '#4f46e5', '#06b6d4', '#10b981', '#f59e0b', '#ef4444',
            '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#3b82f6'
        ];

        const displayLabels = labels.map(formatLabel);
        const n = displayLabels.length;
        const longest = displayLabels.reduce((a, b) => (b && b.length > a ? b.length : a), 0);
        const needsRotation = n > 6 || longest > 8;

        const backgroundColors = type === 'pie'
            ? displayLabels.map((_, i) => palette[i % palette.length])
            : 'rgba(79, 70, 229, 0.75)';

        const data = {
            labels: displayLabels,
            datasets: [{
                label: title || '',
                data: values,
                backgroundColor: backgroundColors,
                borderColor: type === 'line' ? '#4f46e5' : '#4f46e5',
                borderWidth: type === 'line' ? 2 : 1,
                borderRadius: type === 'bar' ? 4 : 0,
                pointRadius: type === 'line' ? 3 : 0,
                pointHoverRadius: 5,
                pointHitRadius: type === 'line' ? 14 : 0,
                tension: 0.3,
                fill: type === 'line' ? { target: 'origin', above: 'rgba(79,70,229,0.08)' } : true
            }]
        };

        const xAxis = {
            ticks: {
                autoSkip: true,
                maxRotation: needsRotation ? 55 : 0,
                minRotation: needsRotation ? 35 : 0,
                maxTicksLimit: Math.min(n, 24),
                font: { size: 11 },
                color: '#475569'
            },
            grid: { display: false },
            border: { color: '#e5e7eb' }
        };

        const yAxis = {
            beginAtZero: true,
            ticks: {
                font: { size: 11 },
                color: '#64748b',
                callback: function (v) {
                    if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toFixed(1) + 'M';
                    if (Math.abs(v) >= 1_000) return (v / 1_000).toFixed(0) + 'k';
                    return v;
                }
            },
            grid: { color: '#f1f5f9' },
            border: { display: false }
        };

        const isZoomable = type !== 'pie';
        const plugins = {
            legend: { display: type === 'pie', position: 'right', labels: { boxWidth: 12, font: { size: 11 } } },
            title: { display: !!title, text: title, font: { size: 13, weight: '600' }, color: '#1f2937', padding: { bottom: 10 } },
            tooltip: {
                backgroundColor: '#0f172a',
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
                        backgroundColor: 'rgba(79,70,229,0.15)',
                        borderColor: 'rgba(79,70,229,0.6)',
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

window.driveeChat = {
    scrollToBottom(id) {
        const el = document.getElementById(id);
        if (el) {
            requestAnimationFrame(() => {
                el.scrollTop = el.scrollHeight;
            });
        }
    }
};
