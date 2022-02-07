from django.forms import ModelForm, TextInput, Textarea

from .models import Tasks


class TaskForm(ModelForm):
    class Meta:
        model = Tasks
        fields = ["title", "task"]
        widgets = {
            "title": TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Введите название задачи',
            }),
            "task": Textarea(attrs={
                'class': 'form-control',
                'placeholder': 'Введите описание задачи',
            }),
        }