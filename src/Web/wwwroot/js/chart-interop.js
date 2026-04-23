window.driveeCharts = (function () {
    const instances = {};

    function destroy(id) {
        if (instances[id]) {
            instances[id].destroy();
            delete instances[id];
        }
    }

    function render(id, type, labels, values, title) {
        const el = document.getElementById(id);
        if (!el) return;
        destroy(id);

        const palette = [
            '#4f46e5', '#06b6d4', '#10b981', '#f59e0b', '#ef4444',
            '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#3b82f6'
        ];

        const backgroundColors = type === 'pie'
            ? labels.map((_, i) => palette[i % palette.length])
            : '#4f46e5';

        const data = {
            labels: labels,
            datasets: [{
                label: title || '',
                data: values,
                backgroundColor: backgroundColors,
                borderColor: type === 'line' ? '#4f46e5' : undefined,
                borderWidth: type === 'line' ? 2 : 0,
                tension: 0.25,
                fill: type === 'line' ? false : true
            }]
        };

        const cfg = {
            type: type === 'line' ? 'line' : type === 'pie' ? 'pie' : 'bar',
            data: data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: type === 'pie' },
                    title: { display: !!title, text: title }
                },
                scales: type === 'pie' ? {} : {
                    y: { beginAtZero: true }
                }
            }
        };

        instances[id] = new Chart(el.getContext('2d'), cfg);
    }

    return { render, destroy };
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
