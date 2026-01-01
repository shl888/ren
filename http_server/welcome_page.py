#!/usr/bin/env python3
"""
欢迎页面HTML内容
"""
import datetime

def get_welcome_page():
    """获取欢迎页面的HTML内容"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>🧠 Brain Core Trading System</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }
            
            body {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                align-items: center;
                padding: 20px;
            }
            
            .container {
                background: rgba(255, 255, 255, 0.95);
                border-radius: 20px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                width: 100%;
                max-width: 800px;
                padding: 40px;
                position: relative;
                overflow: hidden;
            }
            
            .header {
                display: flex;
                align-items: center;
                margin-bottom: 30px;
                border-bottom: 2px solid #eaeaea;
                padding-bottom: 20px;
            }
            
            .logo {
                font-size: 48px;
                margin-right: 20px;
                color: #764ba2;
            }
            
            .title {
                font-size: 32px;
                color: #333;
                font-weight: 700;
                letter-spacing: -0.5px;
            }
            
            .subtitle {
                color: #666;
                font-size: 16px;
                margin-top: 5px;
            }
            
            .status-card {
                background: linear-gradient(to right, #4CAF50, #8BC34A);
                color: white;
                padding: 15px 25px;
                border-radius: 12px;
                margin-bottom: 30px;
                display: flex;
                align-items: center;
                justify-content: space-between;
                animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.9; }
                100% { opacity: 1; }
            }
            
            .status-text {
                font-size: 18px;
                font-weight: 600;
            }
            
            .status-badge {
                background: rgba(255, 255, 255, 0.2);
                padding: 8px 16px;
                border-radius: 20px;
                font-size: 14px;
                font-weight: 600;
                backdrop-filter: blur(10px);
            }
            
            .section {
                margin-bottom: 30px;
            }
            
            .section-title {
                font-size: 20px;
                color: #444;
                margin-bottom: 15px;
                font-weight: 600;
                display: flex;
                align-items: center;
            }
            
            .section-title i {
                margin-right: 10px;
                color: #667eea;
            }
            
            .endpoint-list {
                background: #f8f9fa;
                border-radius: 10px;
                overflow: hidden;
            }
            
            .endpoint-item {
                padding: 16px 20px;
                border-bottom: 1px solid #eaeaea;
                display: flex;
                align-items: center;
                transition: all 0.3s ease;
            }
            
            .endpoint-item:hover {
                background: #edf2f7;
                transform: translateX(5px);
            }
            
            .endpoint-item:last-child {
                border-bottom: none;
            }
            
            .endpoint-method {
                background: #667eea;
                color: white;
                padding: 4px 12px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: 600;
                margin-right: 15px;
                min-width: 60px;
                text-align: center;
            }
            
            .endpoint-path {
                flex: 1;
                font-family: 'Courier New', monospace;
                color: #333;
                font-weight: 500;
            }
            
            .endpoint-desc {
                color: #666;
                font-size: 14px;
                margin-top: 3px;
            }
            
            .security-note {
                background: #fff3cd;
                border-left: 4px solid #ffc107;
                padding: 15px;
                border-radius: 8px;
                margin: 25px 0;
                display: flex;
                align-items: center;
            }
            
            .security-note i {
                color: #ff9800;
                font-size: 24px;
                margin-right: 15px;
            }
            
            .footer {
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #eaeaea;
                text-align: center;
                color: #888;
                font-size: 14px;
            }
            
            .timestamp {
                background: #f1f3f9;
                padding: 10px 15px;
                border-radius: 8px;
                font-family: 'Courier New', monospace;
                color: #555;
                font-size: 13px;
                margin-top: 10px;
                display: inline-block;
            }
            
            .api-key-required {
                background: #dc3545;
                color: white;
                padding: 4px 10px;
                border-radius: 4px;
                font-size: 11px;
                font-weight: 600;
                margin-left: 10px;
                letter-spacing: 0.5px;
            }
            
            .public-access {
                background: #28a745;
                color: white;
                padding: 4px 10px;
                border-radius: 4px;
                font-size: 11px;
                font-weight: 600;
                margin-left: 10px;
                letter-spacing: 0.5px;
            }
            
            @media (max-width: 768px) {
                .container {
                    padding: 25px;
                    margin: 10px;
                }
                
                .header {
                    flex-direction: column;
                    text-align: center;
                }
                
                .logo {
                    margin-right: 0;
                    margin-bottom: 15px;
                }
                
                .endpoint-item {
                    flex-direction: column;
                    align-items: flex-start;
                }
                
                .endpoint-method {
                    margin-bottom: 10px;
                    margin-right: 0;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="logo">🧠</div>
                <div>
                    <div class="title">Brain Core Trading System</div>
                    <div class="subtitle">专业加密货币交易系统后端</div>
                </div>
            </div>
            
            <div class="status-card">
                <div class="status-text">🚀 服务器正在运行</div>
                <div class="status-badge">实时在线</div>
            </div>
            
            <div class="section">
                <div class="section-title">
                    <i>🔓</i> 公开接口
                </div>
                <div class="endpoint-list">
                    <div class="endpoint-item">
                        <span class="endpoint-method">GET</span>
                        <div>
                            <div class="endpoint-path">/public/ping</div>
                            <div class="endpoint-desc">外部监控健康检查（用于保持服务器活跃）</div>
                        </div>
                        <span class="public-access">公开访问</span>
                    </div>
                    <div class="endpoint-item">
                        <span class="endpoint-method">GET</span>
                        <div>
                            <div class="endpoint-path">/health</div>
                            <div class="endpoint-desc">系统详细健康状态检查</div>
                        </div>
                        <span class="public-access">公开访问</span>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <div class="section-title">
                    <i>🔐</i> 受保护接口
                </div>
                <div class="endpoint-list">
                    <div class="endpoint-item">
                        <span class="endpoint-method">GET</span>
                        <div>
                            <div class="endpoint-path">/api/account/{exchange}/balance</div>
                            <div class="endpoint-desc">获取交易所账户余额</div>
                        </div>
                        <span class="api-key-required">需要密码</span>
                    </div>
                    <div class="endpoint-item">
                        <span class="endpoint-method">GET</span>
                        <div>
                            <div class="endpoint-path">/api/account/{exchange}/positions</div>
                            <div class="endpoint-desc">获取持仓信息</div>
                        </div>
                        <span class="api-key-required">需要密码</span>
                    </div>
                    <div class="endpoint-item">
                        <span class="endpoint-method">POST</span>
                        <div>
                            <div class="endpoint-path">/api/trade/{exchange}/order</div>
                            <div class="endpoint-desc">创建交易订单</div>
                        </div>
                        <span class="api-key-required">需要密码</span>
                    </div>
                </div>
            </div>
            
            <div class="security-note">
                <i>⚠️</i>
                <div>
                    <strong>安全提醒：</strong> 所有 <code>/api/</code> 开头的接口都需要在请求头中提供 <code>X-Access-Password</code> 密码才能访问。
                    请确保妥善保管您的访问密码。
                </div>
            </div>
            
            <div class="footer">
                <div>系统版本：1.0.0 | 服务状态：运行正常</div>
                <div class="timestamp">服务器时间：{{timestamp}}</div>
            </div>
        </div>
        
        <script>
            // 自动更新时间显示
            function updateTimestamp() {
                const now = new Date();
                const timestampElement = document.querySelector('.timestamp');
                const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
                timestampElement.innerHTML = `服务器时间：${formattedTime}`;
            }
            
            // 初始更新时间
            updateTimestamp();
            
            // 每分钟更新一次时间
            setInterval(updateTimestamp, 60000);
            
            // 添加点击效果
            document.querySelectorAll('.endpoint-item').forEach(item => {
                item.addEventListener('click', function() {
                    const path = this.querySelector('.endpoint-path').textContent;
                    const method = this.querySelector('.endpoint-method').textContent;
                    const url = window.location.origin + path;
                    
                    if (method === 'GET') {
                        window.open(url, '_blank');
                    } else {
                        alert(`接口：${method} ${path}\\n\\n非GET请求需要在代码中调用。`);
                    }
                });
            });
        </script>
    </body>
    </html>
    """
    
    # 替换时间戳
    html_content = html_content.replace("{{timestamp}}", datetime.datetime.now().isoformat())
    
    return html_content
