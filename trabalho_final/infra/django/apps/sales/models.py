from decimal import Decimal

from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q
from django.utils.text import slugify

from .choices import IdempotencyStatus, OrderStatus, SalesChannel
from common.bases.models.base_models import TimeStampedModel


class Store(TimeStampedModel):
    external_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=128)
    region = models.CharField(max_length=64)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "stores"
        indexes = [
            models.Index(fields=["name"], name="stores_name_idx"),
            models.Index(fields=["region"], name="stores_region_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.external_id} - {self.name}"


class Seller(TimeStampedModel):
    external_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=128)
    store = models.ForeignKey(Store, on_delete=models.PROTECT, related_name="sellers")
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "sellers"
        indexes = [
            models.Index(fields=["name"], name="sellers_name_idx"),
            models.Index(fields=["store"], name="sellers_store_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.external_id} - {self.name}"


class Category(TimeStampedModel):
    name = models.CharField(max_length=100, unique=True)
    slug = models.SlugField(max_length=120, unique=True)

    class Meta:
        db_table = "categories"
        verbose_name_plural = "categories"

    def save(self, *args, **kwargs) -> None:
        if not self.slug:
            self.slug = slugify(self.name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.name


class Product(TimeStampedModel):
    external_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=160)
    category = models.ForeignKey(Category, on_delete=models.PROTECT, related_name="products")
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "products"
        indexes = [
            models.Index(fields=["category"], name="products_category_idx"),
            models.Index(fields=["name"], name="products_name_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.external_id} - {self.name}"


class Order(TimeStampedModel):
    external_id = models.CharField(max_length=64, unique=True)
    event_id = models.UUIDField(unique=True, null=True, blank=True)
    customer_id = models.CharField(max_length=64, blank=True)

    store = models.ForeignKey(Store, on_delete=models.PROTECT, related_name="orders")
    seller = models.ForeignKey(Seller, on_delete=models.PROTECT, related_name="orders")

    channel = models.CharField(max_length=20, choices=SalesChannel.choices)
    status = models.CharField(max_length=16, choices=OrderStatus.choices, default=OrderStatus.PAID)

    source = models.CharField(max_length=20, default="LIVE", blank=True)

    ordered_at = models.DateTimeField()
    ingested_at = models.DateTimeField()
    canceled_at = models.DateTimeField(null=True, blank=True)
    returned_at = models.DateTimeField(null=True, blank=True)

    gross_amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(Decimal("0.00"))])
    discount_amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(Decimal("0.00"))])
    shipping_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"), validators=[MinValueValidator(Decimal("0.00"))])
    net_revenue = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(Decimal("0.00"))])
    cogs = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(Decimal("0.00"))])

    class Meta:
        db_table = "orders"
        indexes = [
            models.Index(fields=["created_at"], name="orders_created_at_idx"),
            models.Index(fields=["ordered_at"], name="orders_ordered_at_idx"),
            models.Index(fields=["seller"], name="orders_seller_idx"),
            models.Index(fields=["store"], name="orders_store_idx"),
            models.Index(fields=["status"], name="orders_status_idx"),
            models.Index(fields=["channel"], name="orders_channel_idx"),
            models.Index(fields=["ordered_at", "status"], name="orders_ordered_status_idx"),
            models.Index(fields=["ordered_at", "seller"], name="orders_ordered_seller_idx"),
            models.Index(fields=["ordered_at", "store"], name="orders_ordered_store_idx"),
            models.Index(fields=["ordered_at", "channel"], name="orders_ordered_channel_idx"),
        ]

    def __str__(self) -> str:
        return self.external_id


class OrderItem(TimeStampedModel):
    order = models.ForeignKey(Order, on_delete=models.PROTECT, related_name="items")
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name="order_items")
    quantity = models.PositiveIntegerField()
    unit_price = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    gross_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    discount_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=Decimal("0.00"),
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    net_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    cost_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        validators=[MinValueValidator(Decimal("0.00"))],
    )

    class Meta:
        db_table = "order_items"
        indexes = [

            models.Index(fields=["product"], name="order_items_product_idx"),
            models.Index(fields=["order"], name="order_items_order_idx"),
        ]
        constraints = [
            models.CheckConstraint(
                check=Q(quantity__gt=0),
                name="order_items_quantity_positive_chk",
            ),
            models.CheckConstraint(
                check=Q(discount_amount__lte=models.F("gross_amount")),
                name="order_items_discount_le_gross_chk",
            ),
        ]

    def __str__(self) -> str:
        return f"order={self.order_id} product={self.product_id}"


class SalesTarget(TimeStampedModel):
    seller = models.ForeignKey(Seller, on_delete=models.CASCADE, related_name="targets")
    period_start = models.DateField()
    period_end = models.DateField()
    target_gmv = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        validators=[MinValueValidator(Decimal("0.00"))],
    )
    target_orders = models.PositiveIntegerField()

    class Meta:
        db_table = "sales_targets"
        constraints = [
            models.UniqueConstraint(
                fields=["seller", "period_start", "period_end"],
                name="sales_targets_seller_period_uniq",
            ),
            models.CheckConstraint(
                check=Q(period_end__gte=models.F("period_start")),
                name="sales_targets_period_valid_chk",
            ),
        ]
        indexes = [
            models.Index(fields=["period_start", "period_end"], name="sales_targets_period_idx"),
            models.Index(fields=["seller"], name="sales_targets_seller_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.seller.external_id} ({self.period_start} to {self.period_end})"


class IdempotencyKey(TimeStampedModel):
    key = models.CharField(max_length=128, unique=True)
    source = models.CharField(max_length=50, blank=True)
    event_id = models.UUIDField(null=True, blank=True, db_index=True)
    resource_type = models.CharField(max_length=50, blank=True)
    resource_id = models.CharField(max_length=64, blank=True)
    payload_hash = models.CharField(max_length=64, blank=True)
    status = models.CharField(max_length=20, choices=IdempotencyStatus.choices, default=IdempotencyStatus.PROCESSING)
    processed_at = models.DateTimeField(null=True, blank=True)
    response_code = models.PositiveSmallIntegerField(null=True, blank=True)
    error_message = models.TextField(blank=True)

    class Meta:
        db_table = "idempotency_keys"
        indexes = [
            models.Index(fields=["event_id"], name="idempotency_event_id_idx"),
            models.Index(fields=["source"], name="idempotency_source_idx"),
            models.Index(fields=["resource_type", "resource_id"], name="idempotency_resource_idx"),
            models.Index(fields=["status"], name="idempotency_status_idx"),
            models.Index(fields=["processed_at"], name="idempotency_processed_at_idx"),
        ]

    def __str__(self) -> str:
        return self.key
