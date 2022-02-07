from django.db import models
from django.utils.translation import gettext as _


# Create your models here.
class Tasks(models.Model):
    title = models.CharField('Название', max_length=50)
    task = models.TextField('Описание')

    def __set__(self):
        return self.title

    class Meta:
        verbose_name = 'Задача'
        verbose_name_plural = 'Задачи'


class AverageCallsReport(models.Model):
    date = models.DateField(_("Дата"), auto_now=True)
    calls_count = models.IntegerField(_("Количество звонков"))
    average_call_time = models.TimeField(_("Среднее время звонка"), auto_now=True)
    average_ivr_time = models.TimeField(_("Среднее время IVR"), auto_now=True)
    average_ringing_time = models.TimeField(_("Среднее время ответа оператора"), auto_now=True)
