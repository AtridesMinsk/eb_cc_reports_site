from django.db import models


# Create your models here.
class Tasks(models.Model):
    title = models.CharField('Название', max_length=50)
    task = models.TextField('Описание')

    def __set__(self):
        return self.title

    class Meta:
        verbose_name = 'Задача'
        verbose_name_plural = 'Задачи'
