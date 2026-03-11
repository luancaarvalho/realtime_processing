from django.db import models


class SalesChannel(models.TextChoices):
    ECOMMERCE = "ecommerce", "E-commerce"
    MARKETPLACE = "marketplace", "Marketplace"
    APP = "app", "Aplicativo"
    PHYSICAL_STORE = "loja_fisica", "Loja fisica"
    
    
class OrderStatus(models.TextChoices):
    PAID = "PAID", "Pago"
    CANCELLED = "CANCELLED", "Cancelado"
    RETURNED = "RETURNED", "Devolvido"
    
    
class IdempotencyStatus(models.TextChoices):
    PROCESSING = "PROCESSING", "Processing"
    PROCESSED = "PROCESSED", "Processed"
    FAILED = "FAILED", "Failed"

