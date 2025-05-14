# 电商用户行为大数据分析平台

基于PySpark和Django开发的大规模电商用户行为分析系统，提供数据可视化展示和深度分析功能。

## 项目概述

本项目是一个电商用户行为分析平台，通过对大规模用户行为数据进行处理和分析，帮助商家了解用户购买模式、商品销售趋势和用户分群特征。

## 主要功能

- 热销商品分析(Top 10商品展示)
- 月度购买趋势分析
- 用户行为聚类分析
- 商品销售预测
- 转化漏斗分析

## 技术栈

### PySpark数据处理
- 使用Spark SQL进行数据清洗和转换
- 采用DataFrame API实现高效ETL流程
- 配置参数：
  - executor-memory: 8G
  - driver-memory: 4G
  - num-executors: 4
- MLlib算法调优：
  - K-Means聚类参数优化
  - 随机森林特征重要性分析

### Django架构设计
- 采用MVT模式组织代码结构
- RESTful API设计规范
- 使用Django ORM进行数据访问
- 异步任务处理使用Celery

### 前端可视化
- 基于Bootstrap 5的响应式布局
- Chart.js实现交互式图表
- 使用WebSocket实时更新数据
- 自定义主题和动画效果

## 安装指南

1. 克隆项目仓库
```bash
git clone https://github.com/your-repo/BigDataSynthsis_CurriculumDesign.git
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 运行开发服务器
```bash
python manage.py runserver
```

## 使用说明

1. 访问 `http://localhost:8000` 进入系统首页
2. 点击各功能模块查看相应分析结果
3. 所有分析结果均以可视化图表形式展示

## 项目截图

![首页截图](static/images/screenshot.png)

## 贡献者