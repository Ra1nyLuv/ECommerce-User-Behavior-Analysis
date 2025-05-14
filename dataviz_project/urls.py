from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('visualization.urls')),  # 将根路径指向visualization应用的URLs
]