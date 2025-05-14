from django.shortcuts import render
import os
from django.conf import settings

# 首页视图，显示所有可视化图表的链接
def index(request):
    return render(request, 'visualization/index.html')

# 各个可视化图表的视图函数
def top_items(request):
    # 读取HTML文件内容
    html_path = os.path.join(settings.BASE_DIR, 'top_items.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        chart_content = f.read()
    
    return render(request, 'visualization/chart_display.html', {
        'title': 'Top 10 商品',
        'chart_content': chart_content,
    })

def monthly_trend(request):
    html_path = os.path.join(settings.BASE_DIR, 'monthly_trend.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        chart_content = f.read()
    
    return render(request, 'visualization/chart_display.html', {
        'title': '月度购买趋势',
        'chart_content': chart_content,
    })

def category_distribution(request):
    html_path = os.path.join(settings.BASE_DIR, 'category_distribution.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        chart_content = f.read()
    
    return render(request, 'visualization/chart_display.html', {
        'title': '商品分类分布',
        'chart_content': chart_content,
    })

def conversion_funnel(request):
    html_path = os.path.join(settings.BASE_DIR, 'conversion_funcel.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        chart_content = f.read()
    
    return render(request, 'visualization/chart_display.html', {
        'title': '用户行为转化漏斗',
        'chart_content': chart_content,
    })

def hourly_trend(request):
    html_path = os.path.join(settings.BASE_DIR, 'hourly_trend.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        chart_content = f.read()
    
    return render(request, 'visualization/chart_display.html', {
        'title': '小时级活跃度',
        'chart_content': chart_content,
    })

def user_clusters(request):
    html_path = os.path.join(settings.BASE_DIR, 'user_clusters.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        chart_content = f.read()
    
    return render(request, 'visualization/chart_display.html', {
        'title': '用户分群分布',
        'chart_content': chart_content,
    })
