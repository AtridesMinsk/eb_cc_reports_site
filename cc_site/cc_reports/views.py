import csv
from django.shortcuts import render, redirect
from .models import Tasks
from .forms import TaskForm


# Create your views here.
def index(request):
    tasks = Tasks.objects.all()
    return render(request, 'cc_reports/index.html', {'title': 'Главная страница сайта', 'tasks': tasks})


def about(request):
    return render(request, 'cc_reports/about.html')


def average_call_rep(request):
    svc_data = []
    with open('cc_data.csv', 'rU') as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            svc_data.append(row)
    return render(request, 'cc_reports/calls_rep.html', {'title': 'Статистика', 'reader': svc_data})


def create(request):
    error = ''
    if request.method == 'POST':
        form = TaskForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('home')
        else:
            error = 'Неверная форма'

    form = TaskForm()
    context = {
        'form': form,
        'error': error
    }
    return render(request, 'cc_reports/create.html', context)
