import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _

class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        # Этот параметр указывает Django, что этот класс не является представлением таблицы
        abstract = True

class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True

class Genre(UUIDMixin, TimeStampedMixin):
    # Первым аргументом обычно идёт человекочитаемое название поля
    name = models.CharField(_('name'), max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return self.name 

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"genre"
        # Следующие два поля отвечают за название модели в интерфейсеca
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'

class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = "content\".\"genre_film_work" 

class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(max_length=255, blank=False, null=False)
    

    def __str__(self):
        return self.full_name 

    class Meta:
        db_table = "content\".\"person" 
        verbose_name = 'Актер'
        verbose_name_plural = 'Актеры'


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField('role')
    created = models.DateTimeField(auto_now_add=True) 

    class Meta:
        db_table = "content\".\"person_film_work" 

class TypeChoices(models.TextChoices):
    MOVIE = 'movie', 'Movie'
    TV_SHOW = 'tv_show', 'TV Show'

class FilmWork(UUIDMixin, TimeStampedMixin):
    # certificate = models.CharField(_('certificate'), max_length=512, blank=True)
    title = models.CharField(_('title'), max_length=255, unique=True)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField()
    type = models.CharField(_('type'),max_length=20,choices=TypeChoices.choices)
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    # file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')
    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    person = models.ManyToManyField(Person, through='PersonFilmWork')

    def __str__(self):
        return self.title 

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = 'Кинопроизведение'
        verbose_name_plural = 'Кинопроизведения'