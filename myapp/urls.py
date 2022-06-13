from django.urls import include, path

from .views import HomeView

urlpatterns = [
    path('', HomeView.as_view(), name='url_items')
]
