#!/usr/bin/env python3
"""
资金费率结算页面模板（正规化改造版）
"""
import datetime
from typing import Any, Dict  # ✅ 修复导入

def get_html_page(manager: Any, contracts: Dict[str, Any]) -> str:
    """
    生成资金费率结算HTML页面（从market_data结构解析）
    contracts结构: {"BTCUSDT": {"funding_settlement": {...}, "latest": "funding_settlement"}, ...}
    """
    
    contracts_html = ""
    for symbol, data_dict in sorted(contracts.items()):
        # ✅ 正确解析：先获取funding_settlement内部数据
        funding_info = data_dict.get('funding_settlement', {})
        raw_data = funding_info.get('raw_data', {})

        
        # ✅ 兼容raw_data格式和简洁格式
        raw_data = funding_info.get('raw_data', {})
        
        # 优先从raw_data中获取，如果没有则从funding_info中获取
        funding_rate = raw_data.get('funding_rate', 0)
        funding_time = raw_data.get('funding_time', 0)
        
        # 计算数据年龄
        if funding_time:
            age_seconds = (datetime.datetime.now().timestamp() * 1000 - funding_time) / 1000
            age_str = f"{int(age_seconds)}秒" if age_seconds < 3600 else f"{int(age_seconds / 3600)}小时"
        else:
            age_str = "未知"
        
        # 格式化费率
        rate_color = "#28a745" if funding_rate >= 0 else "#dc3545"
        rate_str = f"{funding_rate:.6f}"
        time_str = datetime.datetime.fromtimestamp(funding_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if funding_time else 'N/A'
        
        contracts_html += f"""
        <tr>
            <td>{symbol}</td>
            <td style="color: {rate_color}; font-weight: 600;">{rate_str}</td>
            <td>{time_str}</td>
            <td>{age_str}</td>
        </tr>
        """
    
    # 如果没有数据
    if not contracts_html:
        contracts_html = """
        <tr>
            <td colspan="4" style="text-align: center; padding: 40px; color: #666;">
                <div style="font-size: 48px; margin-bottom: 10px;">📊</div>
                <div>暂无数据</div>
                <div style="font-size: 14px; margin-top: 10px;">后台正在获取，请稍候...</div>
            </td>
        </tr>
        """
    
    # 获取状态
    status = manager.get_status()
    last_fetch = status.get('last_fetch_time', '从未')
    is_fetched = status.get('is_auto_fetched', False)
    manual_count = status.get('manual_fetch_count', '0/3')
    weight_info = status.get('api_weight_per_request', 10)
    
    status_badge = "✅ 已获取" if is_fetched else "⏳ 获取中..."
    status_color = "#4CAF50" if is_fetched else "#ff9800"
    
    return f"""
<!DOCTYPE html>
<html>
<head>
    <title>资金费率结算数据 | Brain Core Trading</title>
    <meta charset="utf-8">
    <style>
        * {{margin: 0; padding: 0; box-sizing: border-box; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;}}
        body {{background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px;}}
        .container {{max-width: 1200px; margin: 0 auto; background: rgba(255, 255, 255, 0.95); border-radius: 20px; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); overflow: hidden;}}
        .header {{background: linear-gradient(to right, #667eea, #764ba2); color: white; padding: 30px; text-align: center;}}
        .header h1 {{font-size: 32px; margin-bottom: 10px;}}
        .info-box {{background: #e3f2fd; border-left: 4px solid #2196F3; padding: 15px; margin: 20px 30px; border-radius: 8px; color: #0d47a1;}}
        .status-grid {{display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; padding: 30px; background: #f8f9fa;}}
        .status-card {{background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); border-left: 4px solid #667eea;}}
        .status-card h3 {{color: #667eea; font-size: 14px; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px;}}
        .status-card .value {{font-size: 28px; font-weight: 700; color: #333;}}
        .action-section {{padding: 30px; text-align: center; border-bottom: 1px solid #eaeaea;}}
        .fetch-button {{background: linear-gradient(to right, #4CAF50, #8BC34A); color: white; border: none; padding: 15px 40px; font-size: 16px; font-weight: 600; border-radius: 50px; cursor: pointer; box-shadow: 0 4px 15px rgba(76, 175, 80, 0.3); transition: all 0.3s ease;}}
        .fetch-button:hover {{transform: translateY(-2px); box-shadow: 0 6px 20px rgba(76, 175, 80, 0.4);}}
        .fetch-button:disabled {{background: #ccc; cursor: not-allowed; box-shadow: none;}}
        .data-table {{padding: 30px;}}
        table {{width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);}}
        th {{background: #667eea; color: white; padding: 15px; text-align: left; font-weight: 600;}}
        td {{padding: 12px 15px; border-bottom: 1px solid #eaeaea;}}
        tr:hover {{background: #f8f9fa;}}
        .rate-positive {{color: #28a745; font-weight: 600;}}
        .rate-negative {{color: #dc3545; font-weight: 600;}}
        .footer {{padding: 20px 30px; text-align: center; background: #f8f9fa; color: #666; font-size: 14px;}}
        .loading {{display: none; text-align: center; padding: 40px;}}
        .loading.active {{display: block;}}
        .spinner {{border: 3px solid #f3f3f3; border-top: 3px solid #667eea; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin: 0 auto 20px;}}
        @keyframes spin {{0% {{transform: rotate(0deg);}} 100% {{transform: rotate(360deg);}}}}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 资金费率结算数据</h1>
            <p>币安USDT永续合约最近结算周期（统一从market_data读取）</p>
        </div>
        
        <div class="info-box">
            <strong>💡 说明：</strong> 点击"刷新数据"可立即获取最新数据，每小时最多3次。
        </div>
        
        <div class="status-grid">
            <div class="status-card">
                <h3>数据状态</h3>
                <div class="value" style="color: {status_color};">{status_badge}</div>
                <div>上次获取: {last_fetch}</div>
            </div>
            <div class="status-card">
                <h3>USDT合约</h3>
                <div class="value">{len(contracts)}</div>
                <div>永续合约数量</div>
            </div>
            <div class="status-card">
                <h3>手动刷新</h3>
                <div class="value">{manual_count}</div>
                <div>每小时限制 3次</div>
            </div>
            <div class="status-card">
                <h3>API权重</h3>
                <div class="value">{weight_info}</div>
                <div>每次请求消耗</div>
            </div>
        </div>
        
        <div class="action-section">
            <button class="fetch-button" onclick="fetchData()">🔄 刷新数据</button>
            <div class="loading" id="loading">
                <div class="spinner"></div>
                <p>正在获取最新数据，请稍候...</p>
            </div>
        </div>
        
        <div class="data-table">
            <h2>📈 实时资金费率数据</h2>
            <table>
                <thead>
                    <tr>
                        <th>合约</th>
                        <th>结算费率</th>
                        <th>结算时间</th>
                        <th>数据年龄</th>
                    </tr>
                </thead>
                <tbody>
                    {contracts_html}
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>服务器时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>数据来源: Binance API /fapi/v1/fundingRate | limit=1000</p>
            <p>合约数量: {len(contracts)} USDT永续合约</p>
        </div>
    </div>
    
    <script>
        async function fetchData() {{
            const button = document.querySelector('.fetch-button');
            const loading = document.getElementById('loading');
            
            button.disabled = true;
            loading.classList.add('active');
            
            try {{
                const response = await fetch('/api/funding/settlement/fetch', {{
                    method: 'POST'
                }});
                
                const result = await response.json();
                
                if (result.success) {{
                    alert('✅ 刷新成功！获取 ' + result.filtered_count + ' 个合约');
                    location.reload();
                }} else {{
                    alert('❌ 刷新失败: ' + result.error);
                }}
                
            }} catch (e) {{
                alert('请求错误: ' + e.message);
            }} finally {{
                button.disabled = false;
                loading.classList.remove('active');
            }}
        }}
    </script>
</body>
</html>
"""