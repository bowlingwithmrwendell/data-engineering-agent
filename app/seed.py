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


    # --- Summit Trail Co. (acquired, on Databricks) -------------------------------------------
    r2 = random.Random(7)
    codes = ["NO", "SO", "EA", "WE"]
    summit_customers, crosswalk = [], []
    for i in range(101, 131):
        first, last = _FIRST[(i * 7) % len(_FIRST)], _LAST[(i * 5) % len(_LAST)]
        summit_customers.append({
            "cust_no": f"SC-{i}",
            "customer_name": f"{first} {last}",
            "email_addr": f"{first.lower()}{i}@summit-example.com",
            "territory": "INTL" if i in (117, 126) else codes[r2.randrange(4)],
            "since": (date(2023, 3, 1) + timedelta(days=r2.randrange(900))).isoformat(),
        })
        # 8 Summit customers are already Lakeside customers; the rest get new Lakeside IDs.
        crosswalk.append({"legacy_system": "summit", "legacy_id": f"SC-{i}",
                          "customer_id": f"C-{(i - 100) * 4:03d}" if i <= 108 else f"C-{100 + i:03d}"})
    status_codes = ["Closed"] * 8 + ["Returned", "Void"]
    summit_orders = []
    for i in range(150):
        cust = "SC-131" if i in (12, 77, 140) else f"SC-{r2.randrange(101, 131)}"   # not in the crosswalk
        prod = f"P-{r2.randrange(1, 13):03d}"
        qty = r2.choice([1, 1, 2, 2, 3])
        summit_orders.append({
            "ord_no": f"ST-{5001 + i}",
            "cust_no": cust,
            "item_code": prod,
            "ord_date": (date(2026, 6, 1) + timedelta(days=r2.randrange(130))).isoformat(),
            "order_status": r2.choice(status_codes),
            "qty": qty,
            "total_usd": "N/A" if i in (33, 98) else f"{qty * price[prod]:.2f}",
        })

    return {"customers": customers, "products": products, "products_current": products_current,
            "orders": orders, "web_events": events, "support_tickets": tickets,
            "summit_orders": summit_orders, "summit_customers": summit_customers,
            "customer_crosswalk": crosswalk}


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
        {"id": "summit_orders", "friendly_name": "Summit Orders", "layer": "bronze", "platform": "databricks",
         "owner": "Summit Trail Co. (acquired) · Finance IT", "tags": ["Acquired", "Confidential"],
         "refresh": "daily", "last_loaded": asof,
         "description": ("Orders from Summit Trail Co., acquired in 2026. Their own column names and status codes; "
                         "dates and totals are stored as text."),
         "columns": [
             _col("ord_no", "string", "Summit order number"),
             _col("cust_no", "string", "Summit customer number (map through the Customer ID crosswalk)"),
             _col("item_code", "string", "Product code (same codes as Lakeside)"),
             _col("ord_date", "string", "Order date, stored as text (YYYY-MM-DD)"),
             _col("order_status", "string", "Summit status code", accepted_values=["Closed", "Returned", "Void"]),
             _col("qty", "integer", "Units"),
             _col("total_usd", "string", "Order value in US dollars, stored as text", classification="Confidential"),
         ],
         "relationships": [{"column": "cust_no", "references": "summit_customers.cust_no"}]},
        {"id": "summit_customers", "friendly_name": "Summit Customers", "layer": "bronze", "platform": "databricks",
         "owner": "Summit Trail Co. (acquired) · CRM", "tags": ["Acquired", "PII"], "refresh": "daily",
         "last_loaded": asof,
         "description": "Summit Trail Co. customer accounts. Territory codes differ from Lakeside regions.",
         "columns": [
             _col("cust_no", "string", "Summit customer number"),
             _col("customer_name", "string", "Customer's name", classification="PII"),
             _col("email_addr", "string", "Customer's email address", classification="PII"),
             _col("territory", "string", "Summit territory code", accepted_values=["NO", "SO", "EA", "WE", "INTL"]),
             _col("since", "string", "Account opened, stored as text (YYYY-MM-DD)"),
         ],
         "relationships": []},
        {"id": "customer_crosswalk", "friendly_name": "Customer ID crosswalk", "layer": "silver",
         "platform": "snowflake", "owner": "Master Data Management", "tags": ["Certified", "Internal"],
         "refresh": "daily", "last_loaded": asof,
         "description": ("Maps acquired companies' customer numbers to Lakeside customer IDs. One row per "
                         "legacy ID; customers who already shopped at Lakeside keep their existing ID."),
         "columns": [
             _col("legacy_system", "string", "Which company the legacy ID came from", accepted_values=["summit"]),
             _col("legacy_id", "string", "The acquired company's customer number"),
             _col("customer_id", "string", "The Lakeside customer ID"),
         ],
         "relationships": []},
    ]


def seed(s: Session) -> None:
    """Load the catalog and sample rows once. Safe to call on every start."""
    have = {d.id for d in s.query(DatasetRow.id)}
    entries = [e for e in catalog_entries() if e["id"] not in have]
    if not entries:
        return
    rows = generate_rows()
    for e in entries:
        s.add(DatasetRow(**e, origin="seed", rows=rows[e["id"]]))
    if "products_current" in have:
        s.commit()
        return
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


# --- Migration mode: conform examples and a legacy report ----------------------------------------

_ORDERS_MODEL = [
    {"name": "order_id", "type": "string", "key": True, "required": True, "description": "Unique order number"},
    {"name": "customer_id", "type": "string", "description": "Lakeside customer ID"},
    {"name": "product_id", "type": "string", "description": "Product code"},
    {"name": "order_date", "type": "date", "required": True, "description": "Day the order was placed"},
    {"name": "status", "type": "string", "required": True, "accepted_values": ["completed", "returned", "cancelled"],
     "description": "Standard Lakeside order status"},
    {"name": "quantity", "type": "integer", "description": "Units"},
    {"name": "amount", "type": "decimal", "description": "Order value in US dollars"},
]

CONFORM_EXAMPLES = [
    {"id": "combine-orders", "title": "Combine Summit orders with Lakeside orders",
     "ask": ("We acquired Summit Trail Co. Combine their orders with ours on Snowflake, in our standard order "
             "format, using the customer ID crosswalk."),
     "spec": {"kind": "conform", "name": "All orders (Lakeside + Summit)", "platform": "snowflake",
              "model": _ORDERS_MODEL,
              "sources": [
                  {"dataset": "orders", "label": "lakeside", "mappings": [
                      {"target": "order_id", "source": "order_id"}, {"target": "customer_id", "source": "customer_id"},
                      {"target": "product_id", "source": "product_id"}, {"target": "order_date", "source": "order_date"},
                      {"target": "status", "source": "status"}, {"target": "quantity", "source": "quantity"},
                      {"target": "amount", "source": "amount"}]},
                  {"dataset": "summit_orders", "label": "summit", "mappings": [
                      {"target": "order_id", "source": "ord_no"},
                      {"target": "customer_id", "source": "cust_no",
                       "lookup": {"dataset": "customer_crosswalk", "key": "legacy_id", "value": "customer_id"}},
                      {"target": "product_id", "source": "item_code"},
                      {"target": "order_date", "source": "ord_date"},
                      {"target": "status", "source": "order_status",
                       "value_map": {"Closed": "completed", "Returned": "returned", "Void": "cancelled"}},
                      {"target": "quantity", "source": "qty"},
                      {"target": "amount", "source": "total_usd", "cast": "decimal"}]}],
              "target": {"name": "orders_combined", "layer": "silver"}, "schedule": "daily"}},
    {"id": "combine-customers", "title": "Combine the two customer lists",
     "ask": "Merge Summit's customers into our customer table on Snowflake without duplicating shared customers.",
     "spec": {"kind": "conform", "name": "All customers (Lakeside + Summit)", "platform": "snowflake",
              "model": [
                  {"name": "customer_id", "type": "string", "key": True, "required": True},
                  {"name": "full_name", "type": "string"},
                  {"name": "email", "type": "string"},
                  {"name": "region", "type": "string", "accepted_values": ["North", "South", "East", "West"]},
                  {"name": "segment", "type": "string", "accepted_values": ["Consumer", "Business"]},
                  {"name": "signup_date", "type": "date"}],
              "sources": [
                  {"dataset": "customers", "label": "lakeside", "mappings": [
                      {"target": "customer_id", "source": "customer_id"}, {"target": "full_name", "source": "full_name"},
                      {"target": "email", "source": "email"}, {"target": "region", "source": "region"},
                      {"target": "segment", "source": "segment"}, {"target": "signup_date", "source": "signup_date"}]},
                  {"dataset": "summit_customers", "label": "summit", "mappings": [
                      {"target": "customer_id", "source": "cust_no",
                       "lookup": {"dataset": "customer_crosswalk", "key": "legacy_id", "value": "customer_id"}},
                      {"target": "full_name", "source": "customer_name"},
                      {"target": "email", "source": "email_addr"},
                      {"target": "region", "source": "territory",
                       "value_map": {"NO": "North", "SO": "South", "EA": "East", "WE": "West"}},
                      {"target": "segment", "default": "Consumer"},
                      {"target": "signup_date", "source": "since", "cast": "date"}]}],
              "target": {"name": "customers_combined", "layer": "silver"}, "schedule": "daily"}},
]

LEGACY_EXAMPLE = {
    "id": "summit-territory-report",
    "title": "Summit's old 'units by territory' report",
    "sql": ("-- Summit Trail Co. legacy report (Databricks SQL)\n"
            "SELECT c.territory, SUM(o.qty) AS units, COUNT(*) AS orders\n"
            "FROM summit_lakehouse.sales.summit_orders AS o\n"
            "JOIN summit_lakehouse.sales.summit_customers AS c ON o.cust_no = c.cust_no\n"
            "WHERE o.order_status = 'Closed'\n"
            "GROUP BY c.territory"),
    "spec": {"name": "Summit units by territory", "platform": "databricks", "source": "summit_orders",
             "joins": [{"dataset": "summit_customers", "left": "summit_orders.cust_no",
                        "right": "summit_customers.cust_no", "how": "inner"}],
             "filters": [{"column": "summit_orders.order_status", "op": "=", "value": "Closed"}],
             "group_by": [{"column": "summit_customers.territory"}],
             "measures": [{"agg": "sum", "column": "summit_orders.qty", "name": "units"},
                          {"agg": "count", "column": None, "name": "orders"}],
             "target": {"name": "summit_units_by_territory", "layer": "gold"}, "schedule": "daily"},
}
