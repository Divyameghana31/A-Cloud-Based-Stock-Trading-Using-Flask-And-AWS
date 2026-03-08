// Example chart configuration for dashboard
function initChart(canvasId, labels, data) {
          const ctx = document.getElementById(canvasId).getContext('2d');
          new Chart(ctx, {
                    type: 'line',
                    data: {
                              labels: labels,
                              datasets: [{
                                        label: 'Portfolio Value',
                                        data: data,
                                        borderColor: '#1e90ff',
                                        backgroundColor: 'rgba(30, 144, 255, 0.1)',
                                        fill: true,
                                        tension: 0.4
                              }]
                    },
                    options: {
                              responsive: true,
                              scales: {
                                        y: {
                                                  beginAtZero: true,
                                                  ticks: {
                                                            color: '#212529'
                                                  }
                                        },
                                        x: {
                                                  ticks: {
                                                            color: '#212529'
                                                  }
                                        }
                              },
                              plugins: {
                                        legend: {
                                                  labels: {
                                                            color: '#212529'
                                                  }
                                        }
                              }
                    }
          });
      }   