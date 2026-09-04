$(document).ready(function() {
    let activeTimeframe = $('#timeframe-select').val() || '5m';

    // Range configurations
    const getConfiguredYRange = () => parseFloat($('#timeframe-select option:selected').data('y-range')) || (activeTimeframe === '15m' ? 50 : 30);
    const getConfiguredXRangeBars = () => (parseInt($('#timeframe-select option:selected').data('x-range-minutes'), 10) || 5) * 60;

    let currentYRange = getConfiguredYRange();
    let currentXWindowBars = getConfiguredXRangeBars();

    let chart = null;
    let twapSeries = null;
    let openPriceLine = null;
    let currentOpenPrice = 0;
    let currentYMin = null;
    let currentYMax = null;
    let latestPoint = null;
    let ws = null;
    let loadedPointsCount = 0;
    let userPannedHorizontally = false;

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

    // Initialize TradingView Lightweight Chart (v5 API) - Light Theme
    const chartContainer = document.getElementById('chart-container');
    chart = LightweightCharts.createChart(chartContainer, {
        autoSize: true,
        layout: {
            background: { type: 'solid', color: '#ffffff' },
            textColor: '#475569',
            fontSize: 12
        },
        grid: {
            vertLines: { color: '#f1f5f9' },
            horzLines: { color: '#f1f5f9' }
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
            borderColor: '#e2e8f0',
            scaleMargins: { top: 0, bottom: 0 },
            autoScale: true,
        },
        timeScale: {
            borderColor: '#e2e8f0',
            timeVisible: true,
            secondsVisible: true,
            minBarSpacing: 0.05,
            rightOffset: 0,
        },
        localization: {
            priceFormatter: priceFormatter
        }
    });

    // Add TWAP series (v5 syntax) with sliding window autoscaleInfoProvider
    twapSeries = chart.addSeries(LightweightCharts.LineSeries, {
        color: '#2563eb',
        lineWidth: 2,
        title: '',
        priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
        autoscaleInfoProvider: () => {
            if (currentYMin !== null && currentYMax !== null && currentYMax > currentYMin) {
                return {
                    priceRange: {
                        minValue: currentYMin,
                        maxValue: currentYMax,
                    },
                    margins: {
                        above: 0,
                        below: 0,
                    },
                };
            }
            return null;
        }
    });

    // Function to maintain the latest price pinned to the RIGHT edge of the X-axis
    // If fewer seconds elapsed than the window (e.g. 120s of 300s), the left space (180s) is empty whitespace
    function updateTimeScaleWindow() {
        if (userPannedHorizontally || loadedPointsCount === 0) return;
        const to = loadedPointsCount - 1;
        const from = to - (currentXWindowBars - 1);
        chart.timeScale().setVisibleLogicalRange({ from: from, to: to });
    }

    // Check sliding Y-axis window (10% - 90% boundary check)
    // If price exceeds top 10% or drops below bottom 10%, the window shifts (without stretching)
    function checkSlidingYWindow(price) {
        if (!price || price <= 0 || !currentYRange || currentYRange <= 0) return;

        if (currentYMin === null || currentYMax === null) {
            const half = currentYRange / 2;
            currentYMin = price - half;
            currentYMax = price + half;
            return;
        }

        const margin = 0.10 * currentYRange;
        const upperThreshold = currentYMax - margin; // 10% from top
        const lowerThreshold = currentYMin + margin; // 10% from bottom

        let changed = false;
        if (price > upperThreshold) {
            currentYMax = price + margin;
            currentYMin = currentYMax - currentYRange;
            changed = true;
        } else if (price < lowerThreshold) {
            currentYMin = price - margin;
            currentYMax = currentYMin + currentYRange;
            changed = true;
        }

        if (changed) {
            twapSeries.applyOptions({});
        }
    }

    // Initialize or reset Y-axis bounds around open price
    function resetYBounds(openPrice) {
        currentOpenPrice = parseFloat(openPrice);
        currentYRange = getConfiguredYRange();
        if (currentOpenPrice && currentOpenPrice > 0) {
            const half = currentYRange / 2;
            currentYMin = currentOpenPrice - half;
            currentYMax = currentOpenPrice + half;
        }
        chart.priceScale('right').applyOptions({ autoScale: true });
        twapSeries.applyOptions({});
        updateTargetBadge();
    }

    // Function to update off-screen Target indicator badge on Y-axis
    function updateTargetBadge() {
        requestAnimationFrame(function() {
            if (!currentOpenPrice || currentOpenPrice <= 0 || !twapSeries || !chart) {
                $('#target-badge').hide();
                return;
            }

            const paneH = (chart.paneSize && chart.paneSize(0)) ? chart.paneSize(0).height : (chartContainer.clientHeight - 28);
            const coord = twapSeries.priceToCoordinate(currentOpenPrice);

            if (coord === null || isNaN(coord)) {
                // Fallback if coordinate conversion is unavailable
                if (currentYMax !== null && currentOpenPrice > currentYMax) {
                    $('#target-badge').removeClass('pos-bottom').addClass('pos-top').css('display', 'flex');
                    $('#target-arrow').text('▲');
                } else if (currentYMin !== null && currentOpenPrice < currentYMin) {
                    $('#target-badge').removeClass('pos-top').addClass('pos-bottom').css('display', 'flex');
                    $('#target-arrow').text('▼');
                } else {
                    $('#target-badge').hide();
                }
                return;
            }

            if (coord < 0) {
                // Target (open price) is above current visible range
                $('#target-badge').removeClass('pos-bottom').addClass('pos-top').css('display', 'flex');
                $('#target-arrow').text('▲');
            } else if (coord > paneH) {
                // Target (open price) is below current visible range
                $('#target-badge').removeClass('pos-top').addClass('pos-bottom').css('display', 'flex');
                $('#target-arrow').text('▼');
            } else {
                // Target is within visible view
                $('#target-badge').hide();
            }
        });
    }

    // Subscribe to visible logical range changes on X-axis:
    // Preserves user zoom (stretch / shrink) and detects horizontal panning away from real-time
    chart.timeScale().subscribeVisibleLogicalRangeChange(function(range) {
        if (!range) return;
        const visibleBars = Math.round(range.to - range.from + 1);
        if (visibleBars >= 10) {
            currentXWindowBars = visibleBars;
        }
        if (loadedPointsCount > 0 && range.to < loadedPointsCount - 5) {
            userPannedHorizontally = true;
        } else if (loadedPointsCount > 0 && range.to >= loadedPointsCount - 2) {
            userPannedHorizontally = false;
        }
        updateTargetBadge();
    });

    // Detect user stretching / dragging Y-axis to retain user's chosen span and position
    function captureUserYBounds() {
        if (!twapSeries || !chart) return;
        const paneH = (chart.paneSize && chart.paneSize(0)) ? chart.paneSize(0).height : (chartContainer.clientHeight - 28);
        const topP = twapSeries.coordinateToPrice(0);
        const botP = twapSeries.coordinateToPrice(paneH);
        if (topP !== null && botP !== null && topP > botP) {
            const userSpan = topP - botP;
            if (userSpan > 1) {
                currentYRange = userSpan;
                currentYMin = botP;
                currentYMax = topP;
            }
        }
        updateTargetBadge();
    }

    $(chartContainer).on('mouseup touchend', function() {
        setTimeout(captureUserYBounds, 50);
    });

    $(chartContainer).on('wheel', function() {
        setTimeout(captureUserYBounds, 50);
    });

    $(window).on('resize', function() {
        setTimeout(updateTargetBadge, 100);
    });

    // Clicking target badge resets Y-scale back to open price centering with default range
    $('#target-badge').on('click', function() {
        resetYBounds(currentOpenPrice);
    });

    // Update top-left legend
    function updateLegend(timeSec, twapVal, medianVal) {
        $('#legend-time').text(timeFormatter(timeSec));
        $('#legend-twap').text(priceFormatter(twapVal));
        $('#legend-open').text(priceFormatter(currentOpenPrice));
        $('#legend-median').text(priceFormatter(medianVal));

        const numTwap = parseFloat(twapVal);
        const numOpen = parseFloat(currentOpenPrice);
        if (!isNaN(numTwap) && !isNaN(numOpen) && numOpen > 0) {
            const diff = numTwap - numOpen;
            const pct = (diff / numOpen) * 100;
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
                updateLegend(latestPoint.time, latestPoint.value, latestPoint.medianPrice);
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
        currentOpenPrice = parseFloat(price);
        if (openPriceLine) {
            twapSeries.removePriceLine(openPriceLine);
            openPriceLine = null;
        }
        if (currentOpenPrice && currentOpenPrice > 0) {
            openPriceLine = twapSeries.createPriceLine({
                price: currentOpenPrice,
                color: '#d97706',
                lineWidth: 2,
                lineStyle: LightweightCharts.LineStyle.Dashed,
                axisLabelVisible: true,
                title: ''
            });
        }
        $('#stat-open').text(priceFormatter(price));
    }

    // Fetch initial candle data from REST API
    function loadCandleData(tf) {
        $.getJSON('/api/twap?timeframe=' + tf, function(data) {
            if (!data) return;

            setOpenPriceLine(data.openPrice);
            resetYBounds(data.openPrice);

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
                loadedPointsCount = chartPoints.length;

                chartPoints.forEach(function(p) {
                    checkSlidingYWindow(p.value);
                });
                latestPoint = chartPoints[chartPoints.length - 1];
                updateTimeScaleWindow();

                $('#stat-twap').text(priceFormatter(latestPoint.value));
                $('#stat-median').text(priceFormatter(latestPoint.medianPrice));
                updateLegend(latestPoint.time, latestPoint.value, latestPoint.medianPrice);
                const binanceDate = new Date(latestPoint.time * 1000);
                $('#binance-time').text('Binance Time: ' + binanceDate.toISOString().substring(11, 19) + ' UTC');
                updateTargetBadge();
            } else {
                twapSeries.setData([]);
                loadedPointsCount = 0;
                updateTimeScaleWindow();
                updateTargetBadge();
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
                    // New candle rollover: reset points, reset user zoom to defaults
                    loadedPointsCount = 0;
                    userPannedHorizontally = false;
                    currentXWindowBars = getConfiguredXRangeBars();

                    twapSeries.setData([]);
                    setOpenPriceLine(msg.openPrice);
                    resetYBounds(msg.openPrice);

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
                    loadedPointsCount++;

                    // Check sliding Y-axis window (10% - 90%)
                    checkSlidingYWindow(newPoint.value);

                    $('#stat-twap').text(priceFormatter(newPoint.value));
                    $('#stat-median').text(priceFormatter(newPoint.medianPrice));
                    updateLegend(newPoint.time, newPoint.value, newPoint.medianPrice);

                    const binanceDate = new Date(newPoint.time * 1000);
                    $('#binance-time').text('Binance Time: ' + binanceDate.toISOString().substring(11, 19) + ' UTC');

                    updateTimeScaleWindow();
                    updateTargetBadge();
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
        userPannedHorizontally = false;
        currentXWindowBars = getConfiguredXRangeBars();
        loadCandleData(activeTimeframe);
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ action: 'setTimeframe', timeframe: activeTimeframe }));
        }
    });

    // Initial load
    loadCandleData(activeTimeframe);
    connectWebSocket();
});
