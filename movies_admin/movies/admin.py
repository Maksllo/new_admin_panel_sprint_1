from django.contrib import admin
from .models import Genre
from .models import FilmWork
from .models import GenreFilmWork
from .models import Person
from .models import PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass  

@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    #добавляем поле поиска по имени
    search_fields = ('full_name',)
      

class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    #добавляем поле поиска по имени связь с search_fields
    autocomplete_fields = ('person',)

class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    
@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline,PersonFilmWorkInline) 