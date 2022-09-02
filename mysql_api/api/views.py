from django.http.response import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from .models import Company
import json
import boto3
from .Consultor import AWS_Model,session_dict
from .commands import  conteo_operaciones,dias_promedio_pago_ops_pagadas,umbrales_outliers,tipo_pago_max_volumen,lista_operaciones
# Create your views here.

class CompanyView(View):
    
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        companies=list(Company.objects.values())
        if len(companies)>0:
            datos={'message':"Success",'companies':companies}
        else:
            datos={'message':"Success",'companies':companies}
        return JsonResponse(datos)
    
    def post(self, request):
        #print(request.body)
        jd=json.loads(request.body)
        #Company.objects.create(name=jd['name'],website=jd['website'],foundation=jd['foundation'])
        aws_query=AWS_Model(session_dict,input=jd)
        df=aws_query.bd_to_dataframe()
        #print(df.head())
        Company.objects.create(conteo_operaciones=conteo_operaciones(df),dias_promedio_pago_ops_pagadas=dias_promedio_pago_ops_pagadas(df),
                               umbrales_outliers=umbrales_outliers(df),tipo_pago_max_volumen=tipo_pago_max_volumen(df),lista_operaciones=lista_operaciones(df))
        datos={'conteo_operaciones':conteo_operaciones(df),'dias_promedio_pago_ops_pagadas':dias_promedio_pago_ops_pagadas(df),
                               'umbrales_outliers':umbrales_outliers(df),'tipo_pago_max_volumen':tipo_pago_max_volumen(df),'lista_operaciones':lista_operaciones(df)}
        print(jd["client_id"])
        return JsonResponse(datos)
    
    def put(self, request):
        pass

    def delete(self, request):
        pass
