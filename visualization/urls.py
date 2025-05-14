from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('top-items/', views.top_items, name='top_items'),
    path('monthly-trend/', views.monthly_trend, name='monthly_trend'),
    path('category-distribution/', views.category_distribution, name='category_distribution'),
    path('conversion-funnel/', views.conversion_funnel, name='conversion_funnel'),
    path('hourly-trend/', views.hourly_trend, name='hourly_trend'),
    path('user-clusters/', views.user_clusters, name='user_clusters'),
]