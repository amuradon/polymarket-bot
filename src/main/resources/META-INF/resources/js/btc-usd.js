$(document).ready(function() {
    let activeTimeframe = $('#timeframe-select').val() || '5m';
    let chart = null;
    let twapSeries = null;
    let openPriceLine = null;
    let currentOpenPrice = 0;
    let latestPoint = null;
    let ws = null;

    // Formatter for currency
    const priceFormatter = (val) => {
        if (val === null || val === undefined || isNaN(val)) return '-';
        return '$' + parseFloat(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    const timeFormatter = (epochSec) => {
        if (!epochSec) return '-';
        const d = new Date(epochSec * 1000);
        return d.toISOString().substring(11, 19) + ' UTC';
    };

    // Initialize TradingView Lightweight Chart (v5 API)
    const chartContainer = document.getElementById('chart-container');
    chart = LightweightCharts.createChart(chartContainer, {
        autoSize: true,
        layout: {
            background: { type: 'solid', color: '#1a1d28' },
            textColor: '#848e9c',
            fontSize: 12
        },
        grid: {
            vertLines: { color: 'rgba(43, 49, 69, 0.4)' },
            horzLines: { color: 'rgba(43, 49, 69, 0.4)' }
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
            borderColor: '#2b3145',
            scaleMargins: { top: 0.1, bottom: 0.1 },
        },
        timeScale: {
            borderColor: '#2b3145',
            timeVisible: true,
            secondsVisible: true,
        },
        localization: {
            priceFormatter: priceFormatter
        }
    });

    // Add TWAP series (v5 syntax)
    twapSeries = chart.addSeries(LightweightCharts.LineSeries, {
        color: '#2962ff',
        lineWidth: 2,
        title: 'TWAP',
        priceFormat: { type: 'price', precision: 2, minMove: 0.01 }
    });

    // Update top-left legend
    function updateLegend(timeSec, twapVal, medianVal) {
        $('#legend-time').text(timeFormatter(timeSec));
        $('#legend-twap').text(priceFormatter(twapVal));
        $('#legend-open').text(priceFormatter(currentOpenPrice));
        $('#legend-median').text(priceFormatter(medianVal));

        if (twapVal !== null && currentOpenPrice > 0) {
            const diff = twapVal - currentOpenPrice;
            const pct = (diff / currentOpenPrice) * 100;
            const sign = diff >= 0 ? '+' : '';
            const colorClass = diff >= 0 ? 'text-green' : 'text-red';
            $('#legend-diff').attr('class', 'legend-val ' + colorClass)
                             .text(sign + diff.toFixed(2) + ' (' + sign + pct.toFixed(2) + '%)');
        } else {
            $('#legend-diff').attr('class', 'legend-val').text('-');
        }
    }

    // Crosshair hover subscription
    chart.subscribeCrosshairMove(function(param) {
        if (!param || param.time === undefined || !param.seriesData) {
            if (latestPoint) {
                updateLegend(latestPoint.time, latestPoint.twap, latestPoint.medianPrice);
            }
            return;
        }
        const twapData = param.seriesData.get(twapSeries);
        if (twapData && twapData.value !== undefined) {
            updateLegend(param.time, twapData.value, twapData.medianPrice || twapData.value);
        }
    });

    // Update open price horizontal line
    function setOpenPriceLine(price) {
        currentOpenPrice = price;
        if (openPriceLine) {
            twapSeries.removePriceLine(openPriceLine);
            openPriceLine = null;
        }
        if (price && price > 0) {
            openPriceLine = twapSeries.createPriceLine({
                price: parseFloat(price),
                color: '#f59e0b',
                lineWidth: 2,
                lineStyle: LightweightCharts.LineStyle.Dashed,
                axisLabelVisible: true,
                title: 'OPEN'
            });
        }
        $('#stat-open').text(priceFormatter(price));
    }

    // Fetch initial candle data from REST API
    function loadCandleData(tf) {
        $.getJSON('/api/twap?timeframe=' + tf, function(data) {
            if (!data) return;

            setOpenPriceLine(data.openPrice);

            const startTimeStr = new Date(data.candleStart * 1000).toISOString().substring(11, 19);
            const endTimeStr = new Date(data.candleEnd * 1000).toISOString().substring(11, 19);
            $('#stat-interval').text(startTimeStr + ' - ' + endTimeStr + ' UTC (' + tf + ')');

            if (data.points && data.points.length > 0) {
                const chartPoints = data.points.map(function(p) {
                    return {
                        time: p.time,
                        value: parseFloat(p.twap),
                        medianPrice: parseFloat(p.medianPrice)
                    };
                });
                twapSeries.setData(chartPoints);
                chart.timeScale().fitContent();

                latestPoint = chartPoints[chartPoints.length - 1];
                $('#stat-twap').text(priceFormatter(latestPoint.value));
                $('#stat-median').text(priceFormatter(latestPoint.medianPrice));
                updateLegend(latestPoint.time, latestPoint.value, latestPoint.medianPrice);
            } else {
                twapSeries.setData([]);
            }
        }).fail(function(err) {
            console.error('Error loading candle data:', err);
        });
    }

    // Setup WebSocket connection to /ws/twap
    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = protocol + '//' + window.location.host + '/ws/twap';

        ws = new WebSocket(wsUrl);

        ws.onopen = function() {
            $('#ws-status').text('CONNECTED').css('color', 'var(--accent-green)');
            // Send active timeframe subscription
            ws.send(JSON.stringify({ action: 'setTimeframe', timeframe: activeTimeframe }));
        };

        ws.onmessage = function(event) {
            try {
                const msg = JSON.parse(event.data);
                if (!msg || msg.timeframe !== activeTimeframe) {
                    return;
                }

                if (msg.isNewCandle) {
                    // New candle rollover: clear old candle data from chart
                    twapSeries.setData([]);
                    setOpenPriceLine(msg.openPrice);
                    const startTimeStr = new Date(msg.candleStart * 1000).toISOString().substring(11, 19);
                    const endTimeStr = new Date(msg.candleEnd * 1000).toISOString().substring(11, 19);
                    $('#stat-interval').text(startTimeStr + ' - ' + endTimeStr + ' UTC (' + msg.timeframe + ')');
                }

                if (msg.point) {
                    const newPoint = {
                        time: msg.point.time,
                        value: parseFloat(msg.point.twap),
                        medianPrice: parseFloat(msg.point.medianPrice)
                    };
                    twapSeries.update(newPoint);
                    latestPoint = newPoint;

                    $('#stat-twap').text(priceFormatter(newPoint.value));
                    $('#stat-median').text(priceFormatter(newPoint.medianPrice));
                    updateLegend(newPoint.time, newPoint.value, newPoint.medianPrice);
                }
            } catch (e) {
                console.error('Error processing websocket message:', e);
            }
        };

        ws.onclose = function() {
            $('#ws-status').text('DISCONNECTED').css('color', 'var(--accent-red)');
            setTimeout(connectWebSocket, 3000);
        };

        ws.onerror = function(err) {
            console.error('WebSocket error:', err);
            ws.close();
        };
    }

    // Handle timeframe dropdown switch
    $('#timeframe-select').on('change', function() {
        activeTimeframe = $(this).val();
        loadCandleData(activeTimeframe);
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ action: 'setTimeframe', timeframe: activeTimeframe }));
        }
    });

    // Initial load
    loadCandleData(activeTimeframe);
    connectWebSocket();
});
