"""
URL configuration for ugatu_software_design project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

from lab_app.views import home_page, register_request, login_request, \
    logout_request, all_pairs_request, spread_today_request

urlpatterns = [
    path('admin/', admin.site.urls),
    # my added urls
    path('', home_page, name='home'),
    path('register/', register_request, name='register'),
    path('login', login_request, name='login'),
    path('logout', logout_request, name='logout'),
    path('all_pairs', all_pairs_request, name='all_pairs'),
    path('spread_today', spread_today_request, name='spread_today'),
]
