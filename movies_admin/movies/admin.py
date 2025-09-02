from django.contrib import admin

from django.contrib import admin
from .models import Genre
from .models import FilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass  

@admin.register(FilmWork)
class FilmWork(admin.ModelAdmin):
    pass  