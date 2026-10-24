"""Fictional demo data estate for "Lakeside Outfitters", an outdoor-gear retailer.

Raw data is spread across three platforms, the way it is in most enterprises:
  * Snowflake  -- orders, customers (the commerce system)
  * Databricks -- products, products_current (the product lakehouse)
  * Fabric     -- web_events, support_tickets (digital and service data)

The sample rows are generated deterministically (fixed random seed), and a few
real-world problems are planted on purpose so the sandbox has something to find:
  * products has a duplicate row for P-007 (a price change), so joining orders to
    raw products repeats rows -- products_current is the deduplicated version
  * 6 orders point at customer C-099, who does not exist (orphans)
  * 2 customers have no region
  * web_events was last loaded days before the as-of date (stale)
  * customers.full_name and customers.email are tagged PII

No real people, companies or figures.
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from sqlalchemy.orm import Session

from . import config
from .models import DatasetRow, LineageRow

_FIRST = ["Ava", "Ben", "Cora", "Dev", "Ella", "Finn", "Gia", "Hugo", "Isla", "Jude", "Kai", "Lena",
          "Milo", "Nia", "Owen", "Pia", "Quin", "Rosa", "Sam", "Tess"]
_LAST = ["Alder", "Brook", "Cedar", "Dale", "Ember", "Frost", "Glen", "Heath", "Ivy", "Juniper"]
_REGIONS = ["North", "South", "East", "West"]
_PRODUCTS = [
    ("P-001", "Trail Runner Shoe", "Footwear", 129.00),
    ("P-002", "Summit Hiking Boot", "Footwear", 189.00),
    ("P-003", "River Sandal", "Footwear", 64.00),
    ("P-004", "Storm Shell Jacket", "Outerwear", 249.00),
    ("P-005", "Down Puffer Vest", "Outerwear", 159.00),
    ("P-006", "Fleece Pullover", "Outerwear", 89.00),
    ("P-007", "Two-Person Tent", "Camping", 329.00),
    ("P-008", "Ultralight Sleeping Bag", "Camping", 279.00),
    ("P-009", "Camp Stove", "Camping", 99.00),
    ("P-010", "Trekking Poles", "Accessories", 79.00),
    ("P-011", "Insulated Bottle", "Accessories", 34.00),
    ("P-012", "Headlamp", "Accessories", 49.00),
]


def _col(name, type_, description, classification=None, accepted_values=None):
    c = {"name": name, "type": type_, "description": description}
    if classification:
        c["classification"] = classification
    if accepted_values:
        c["accepted_values"] = accepted_values
    return c


def generate_rows() -> dict[str, list[dict]]:
    rng = random.Random(42)

    customers = []
    for i in range(1, 41):
        first, last = _FIRST[(i - 1) % len(_FIRST)], _LAST[(i * 3) % len(_LAST)]
        customers.append({
            "customer_id": f"C-{i:03d}",
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "region": None if i in (17, 33) else _REGIONS[rng.randrange(4)],
            "segment": "Business" if rng.random() < 0.25 else "Consumer",
            "signup_date": (date(2024, 1, 1) + timedelta(days=rng.randrange(700))).isoformat(),
        })

    products = [{"product_id": p, "product_name": n, "category": c, "unit_price": pr}
                for p, n, c, pr in _PRODUCTS]
    # Planted problem: an old price row for P-007 that was never cleaned up.
    products.insert(7, {"product_id": "P-007", "product_name": "Two-Person Tent", "category": "Camping",
                        "unit_price": 299.00})
    products_current = [{"product_id": p, "product_name": n, "category": c, "unit_price": pr}
                        for p, n, c, pr in _PRODUCTS]
    price = {p: pr for p, _, _, pr in _PRODUCTS}

    orders = []
    start = date(2026, 1, 1)
    for i in range(400):
        cust = "C-099" if i % 67 == 13 else f"C-{rng.randrange(1, 41):03d}"   # 6 orphans
        prod = f"P-{rng.randrange(1, 13):03d}"
        qty = rng.choice([1, 1, 1, 2, 2, 3, 4])
        r = rng.random()
        status = "completed" if r < 0.80 else ("returned" if r < 0.88 else "cancelled")
        orders.append({
            "order_id": f"O-{10001 + i}",
            "customer_id": cust,
            "product_id": prod,
            "order_date": (start + timedelta(days=rng.randrange(273))).isoformat(),
            "status": status,
            "quantity": qty,
            "amount": round(qty * price[prod], 2),
        })

    events = []
    ev_start = datetime(2026, 9, 1)
    for i in range(600):
        anon = rng.random() < 0.3
        events.append({
            "event_id": f"E-{50001 + i}",
            "customer_id": None if anon else f"C-{rng.randrange(1, 41):03d}",
            "event_type": rng.choices(["page_view", "add_to_cart", "checkout"], [70, 20, 10])[0],
            "event_ts": (ev_start + timedelta(minutes=rng.randrange(60 * 24 * 44))).strftime(
                "%Y-%m-%d %H:%M:%S"),
            "channel": rng.choice(["web", "web", "mobile", "email"]),
        })

    tickets = []
    for i in range(120):
        tickets.append({
            "ticket_id": f"T-{7001 + i}",
            "customer_id": f"C-{rng.randrange(1, 41):03d}",
            "opened_date": (date(2026, 4, 1) + timedelta(days=rng.randrange(200))).isoformat(),
            "priority": rng.choices(["low", "medium", "high"], [50, 35, 15])[0],
            "csat_score": None if rng.random() < 0.15 else rng.choice([2, 3, 4, 4, 5, 5]),
            "channel": rng.choice(["chat", "email", "phone"]),
        })

    return {"customers": customers, "products": products, "products_current": products_current,
            "orders": orders, "web_events": events, "support_tickets": tickets}


def catalog_entries() -> list[dict]:
    asof = config.AS_OF_DATE
    return [
        {"id": "orders", "friendly_name": "Orders", "layer": "bronze", "platform": "snowflake",
         "owner": "Commerce Data Team", "tags": ["Confidential", "Finance"], "refresh": "daily",
         "last_loaded": asof,
         "description": "Every online order: who bought what, when, and for how much. One row per order.",
         "columns": [
             _col("order_id", "string", "Unique order number"),
             _col("customer_id", "string", "Who placed the order (links to Customers)"),
             _col("product_id", "string", "What was bought (links to Products)"),
             _col("order_date", "date", "Day the order was placed"),
             _col("status", "string", "Where the order ended up",
                  accepted_values=["completed", "returned", "cancelled"]),
             _col("quantity", "integer", "Number of units"),
             _col("amount", "decimal", "Order value in US dollars", classification="Confidential"),
         ],
         "relationships": [{"column": "customer_id", "references": "customers.customer_id"},
                           {"column": "product_id", "references": "products_current.product_id"}]},
        {"id": "customers", "friendly_name": "Customers", "layer": "bronze", "platform": "snowflake",
         "owner": "Commerce Data Team", "tags": ["PII", "Confidential"], "refresh": "daily",
         "last_loaded": asof,
         "description": "Customer accounts with region and segment. Contains personal information.",
         "columns": [
             _col("customer_id", "string", "Unique customer number"),
             _col("full_name", "string", "Customer's name", classification="PII"),
             _col("email", "string", "Customer's email address", classification="PII"),
             _col("region", "string", "Sales region", accepted_values=_REGIONS),
             _col("segment", "string", "Consumer or Business customer",
                  accepted_values=["Consumer", "Business"]),
             _col("signup_date", "date", "Day the account was opened"),
         ],
         "relationships": []},
        {"id": "products", "friendly_name": "Products (raw)", "layer": "bronze", "platform": "databricks",
         "owner": "Merchandising Analytics", "tags": ["Internal"], "refresh": "daily",
         "last_loaded": asof - timedelta(days=1), "curated_version": "products_current",
         "description": ("Raw product feed, including old price rows. Not unique per product: use "
                         "Products (current) for joins."),
         "columns": [
             _col("product_id", "string", "Product code (can repeat in this raw feed)"),
             _col("product_name", "string", "Product name"),
             _col("category", "string", "Product category",
                  accepted_values=["Footwear", "Outerwear", "Camping", "Accessories"]),
             _col("unit_price", "decimal", "List price in US dollars"),
         ],
         "relationships": []},
        {"id": "products_current", "friendly_name": "Products (current)", "layer": "silver",
         "platform": "databricks", "owner": "Merchandising Analytics", "tags": ["Internal", "Certified"],
         "refresh": "daily", "last_loaded": asof,
         "description": "One row per product with today's price. The certified table for joins.",
         "columns": [
             _col("product_id", "string", "Unique product code"),
             _col("product_name", "string", "Product name"),
             _col("category", "string", "Product category",
                  accepted_values=["Footwear", "Outerwear", "Camping", "Accessories"]),
             _col("unit_price", "decimal", "Current list price in US dollars"),
         ],
         "relationships": []},
        {"id": "web_events", "friendly_name": "Website & App Events", "layer": "bronze",
         "platform": "fabric", "owner": "Digital Analytics", "tags": ["Internal"], "refresh": "daily",
         "last_loaded": asof - timedelta(days=6),
         "description": "Clicks from the website, mobile app and email campaigns. One row per event.",
         "columns": [
             _col("event_id", "string", "Unique event number"),
             _col("customer_id", "string", "Signed-in customer, blank for anonymous visitors"),
             _col("event_type", "string", "What happened",
                  accepted_values=["page_view", "add_to_cart", "checkout"]),
             _col("event_ts", "timestamp", "When it happened"),
             _col("channel", "string", "Where it happened", accepted_values=["web", "mobile", "email"]),
         ],
         "relationships": [{"column": "customer_id", "references": "customers.customer_id"}]},
        {"id": "support_tickets", "friendly_name": "Support Tickets", "layer": "bronze",
         "platform": "fabric", "owner": "Customer Care", "tags": ["Internal"], "refresh": "daily",
         "last_loaded": asof,
         "description": "Customer support requests with priority and satisfaction score (1 to 5).",
         "columns": [
             _col("ticket_id", "string", "Unique ticket number"),
             _col("customer_id", "string", "Customer who asked for help"),
             _col("opened_date", "date", "Day the ticket was opened"),
             _col("priority", "string", "Urgency", accepted_values=["low", "medium", "high"]),
             _col("csat_score", "integer", "Satisfaction score, 1 (poor) to 5 (great); blank if no reply"),
             _col("channel", "string", "How the customer reached us",
                  accepted_values=["chat", "email", "phone"]),
         ],
         "relationships": [{"column": "customer_id", "references": "customers.customer_id"}]},
    ]


def seed(s: Session) -> None:
    """Load the catalog and sample rows once. Safe to call on every start."""
    if s.query(DatasetRow).filter(DatasetRow.origin == "seed").first():
        return
    rows = generate_rows()
    for e in catalog_entries():
        s.add(DatasetRow(**e, origin="seed", rows=rows[e["id"]]))
    s.add(LineageRow(source="products", target="products_current", pipeline_id=None,
                     column_map=[{"output": c, "from": f"products.{c}", "transform": "latest row per product_id"}
                                 for c in ("product_id", "product_name", "category", "unit_price")]))
    s.commit()


# --- Starter requests: plain-English examples that load into the builder ------------

EXAMPLES = [
    {"id": "monthly-revenue",
     "title": "Monthly revenue by region",
     "ask": "Show completed revenue by region for each month, on Snowflake.",
     "spec": {"name": "Monthly revenue by region", "platform": "snowflake", "source": "orders",
              "joins": [{"dataset": "customers", "left": "orders.customer_id",
                         "right": "customers.customer_id", "how": "left"}],
              "filters": [{"column": "orders.status", "op": "=", "value": "completed"}],
              "group_by": [{"column": "customers.region"},
                           {"column": "orders.order_date", "grain": "month"}],
              "measures": [{"agg": "sum", "column": "orders.amount", "name": "revenue"},
                           {"agg": "count", "column": None, "name": "orders"}],
              "target": {"name": "monthly_revenue_by_region", "layer": "gold"},
              "schedule": "daily"}},
    {"id": "category-sales",
     "title": "Sales by product category",
     "ask": "Total units and revenue by product category, on Databricks.",
     "spec": {"name": "Sales by product category", "platform": "databricks", "source": "orders",
              "joins": [{"dataset": "products", "left": "orders.product_id",
                         "right": "products.product_id", "how": "inner"}],
              "filters": [{"column": "orders.status", "op": "=", "value": "completed"}],
              "group_by": [{"column": "products.category"}],
              "measures": [{"agg": "sum", "column": "orders.quantity", "name": "units"},
                           {"agg": "sum", "column": "orders.amount", "name": "revenue"}],
              "target": {"name": "sales_by_category", "layer": "gold"},
              "schedule": "daily"}},
    {"id": "customer-list",
     "title": "Customer list for the CRM team",
     "ask": "A list of Business customers with their email and region, on Snowflake.",
     "spec": {"name": "Business customer list", "platform": "snowflake", "source": "customers",
              "filters": [{"column": "customers.segment", "op": "=", "value": "Business"}],
              "columns": ["customers.customer_id", "customers.full_name", "customers.email",
                          "customers.region"],
              "target": {"name": "business_customers", "layer": "silver"},
              "schedule": "weekly"}},
    {"id": "support-csat",
     "title": "Support satisfaction by channel",
     "ask": "Average satisfaction score and ticket count by channel and priority, on Fabric.",
     "spec": {"name": "Support satisfaction by channel", "platform": "fabric",
              "source": "support_tickets",
              "group_by": [{"column": "support_tickets.channel"}, {"column": "support_tickets.priority"}],
              "measures": [{"agg": "avg", "column": "support_tickets.csat_score", "name": "avg_csat"},
                           {"agg": "count", "column": None, "name": "tickets"}],
              "target": {"name": "support_csat_by_channel", "layer": "gold"},
              "schedule": "daily"}},
    {"id": "funnel",
     "title": "Digital funnel by channel",
     "ask": "Count of each event type by channel per week, on Fabric.",
     "spec": {"name": "Weekly digital funnel", "platform": "fabric", "source": "web_events",
              "group_by": [{"column": "web_events.event_ts", "grain": "week"},
                           {"column": "web_events.channel"}, {"column": "web_events.event_type"}],
              "measures": [{"agg": "count", "column": None, "name": "events"},
                           {"agg": "count_distinct", "column": "web_events.customer_id",
                            "name": "known_customers"}],
              "target": {"name": "weekly_funnel", "layer": "gold"},
              "schedule": "daily"}},
]
