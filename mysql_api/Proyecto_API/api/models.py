from django.db import models

# Create your models here.
class Company(models.Model):
    id_deudor=models.CharField(max_length=50)
    conteo_operaciones= models.CharField(max_length=100)
    dias_promedio_pago_ops_pagadas= models.PositiveBigIntegerField()
    umbrales_outliers=models.CharField(max_length=100)
    tipo_pago_max_volumen=models.CharField(max_length=100)
    lista_operaciones=models.CharField(max_length=100)


